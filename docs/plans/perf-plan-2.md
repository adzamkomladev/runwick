# Making Runwick faster, round two: the plan

*Draft for review. Written 2026-11-10 against main at `e8a3fbf` (schema revision 20, version 0.1.3). Not approved. Nothing here changes the spec on its own: every spec change still goes into `docs/spec/` with a date.*

**Second review, 2026-11-11:** prioritize large PostgreSQL and Redis gains. This revision adds a critical path, measurement gates, replay and overload requirements, and upstream-source notes. It authorizes no implementation or optional durability change; proposed gains remain unmeasured.

This plan replaces the "what next" part of `docs/plans/perf-plan-draft.md`, whose steps are all done. It is written to be reviewed by a second reader, so every claim says whether it was measured, read in the code, or reasoned from the code.

## How to read and review this

- **Measured** means a benchmark or trace produced it. The source report is named.
- **Verified** means it was read in the code on 2026-11-10. A file and line is given.
- **Inferred** means it follows from the code but nobody has measured it. Phase 0 exists to turn the important inferences into measurements before any code is written on top of them.
- Every speed number so far comes from the shared 8-CPU machine, which is not a reference machine. Swings of 30 to 50 percent between runs are normal there.
- Section 12 lists the questions I most want a reviewer to attack.

## 1. Where we stand

### Speed today (measured)

| Test | Runwick | Best comparable | Source |
|---|---|---|---|
| Add one job at a time, 16 callers, PostgreSQL | 306/s | River 1,711/s, pg-boss 1,316/s | `perf-investigation.md`, quiet host |
| Add one job at a time, 1 caller | 63/s | River 158/s, pg-boss 178/s | same |
| Bulk add of 20,000 jobs | 5.1 s (about 3,900/s) | River 0.8 to 1.3 s, pg-boss 1.6 s | same |
| Drain 20,000 jobs, 16 workers | 188/s | River default 157/s, pg-boss batched 851/s | same |
| Drain 20,000 jobs, 64 workers | 381/s | River tuned 3,621/s | same |
| P-01: 100 jobs/s arriving, 0.1 s jobs | 87.5/s, backlog grows, due-to-start p99 23 s | goal 100/s, p99 2 s | `sp-029.md` |
| P-06: scheduler lag p99 | 52 s | rule 13 s | same |
| P-09: outbox relay | 24 to 26/s | goal 100/s | same, `pf-final.md` |
| Memory allocated per job | about 120 KB | goal 40 KiB | `sp-029.md` |
| 1 MiB payload | producer allocates 23 times the payload, worker 8 times | no goal set | same |
| Redis, add from 16 callers | 332/s | BullMQ 925/s at matching persistence settings (command guarantees differ) | `queue-shootout.md` (unmerged branch) |
| Redis, drain with 64 workers | 241/s | BullMQ 1,754/s at matching persistence settings (command guarantees differ) | same |
| SQLite, drain with 16 workers | about 36/s | not compared | same |

### What round one already did

A job went from 64 trips to the database down to 8. Claims, finishes and lease renewals are batched (up to 32, 32 and 64 per call). Workers are woken when work arrives. Details: `docs/reports/pf-final.md`.

## 2. What the study found

Four studies were done on 2026-11-10: Runwick's PostgreSQL functions, Runwick's producer, worker and relay code, the River and pg-boss sources, and the BullMQ, Laravel queue and Horizon sources together with Runwick's Redis store. Nothing was run in these studies; they read source only.

### Finding 1: trip count alone does not explain the gap

River uses more trips per add than Runwick (begin, insert, sometimes notify, commit) and is still 5.6 times faster. This does not prove trips are cheap or that there is no further gain from combining operations. Measure round-trip latency alongside four other things: how much is written per job, how many jobs share one commit, how long worker slots sit idle, and how much memory each job churns.

### Finding 2: Runwick writes far more per job than the others

For one plain job from add to finish (verified by reading the SQL; the totals are counts from the code, not measured WAL):

| | Runwick | River | pg-boss |
|---|---|---|---|
| Rows inserted | about 10 | 1 | 1 |
| Row updates | about 7 | 2 | 2 |
| Index entries written | about 55 | 6 on insert | about 4 on insert |
| Tables touched | 6 | 1 | 1 |

Where Runwick's writes go (the percentages below are rough logical-write shares, not measured CPU, WAL, latency or achievable speedup; validate the counts against the exact schema):

- **Event rows: about 30 percent.** Four `queue_events` rows per job (dispatched, reserved, started, finished), each with 4 indexes.
- **Command receipts: about 25 percent.** One receipt row per add and one per finished job, with 3 indexes and a check that runs at commit.
- **The separate generations table: about 12 percent.** One insert and one update per job, plus two foreign-key checks.
- **Dashboard indexes on the jobs table.** The table has 10 indexes (`0020_stats_indexes.sql` added the tenth). Seven match a new job. Several are rewritten on every state change, so each job pays for them three times.
- **Small duplicate writes.** Every finished attempt row is updated twice, once to close it and once for resource usage (`0018_attempt_usage.sql:52-57`). Every worker heartbeat updates its row twice (`0019_worker_detail.sql:34-36`).
- **Fencing a stale worker costs under 2 percent.** It is a few extra columns and conditions, with no rows of its own.

No table sets a fill factor or autovacuum setting, and finished jobs stay in the same table and indexes as waiting ones (verified by search of `src/Runwick.Postgres/Sql/`).

### Finding 3: jobs do not share commits

- **Single adds are never grouped by Runwick (verified).** Each add is its own transaction. Do not equate transactions with physical flushes: database group commit can share a flush across concurrent transactions; measure commits and flushes separately. River and pg-boss have the same shape but write one row each, so their commits are cheap.
- **The producer already has a bounded wait queue (verified), but times out waiting callers.** `LocalAdmission` lets 8 calls run and 64 wait for at most 1 second, then throws (`LocalAdmission.cs:4-44`). That is the "local limit" that refused up to half the offers in P-01.
- **The bulk cap of 100 is a client-side constant (verified).** It is in `PayloadBounds.cs:16`. The SQL function has no limit. The comparison harness sends River chunks of 5,000 through its COPY API; that is a harness choice, not a universal River limit. Its COPY API has different uniqueness-conflict semantics (see §14).
- **The wake message is sent inside every add transaction (measured).** PostgreSQL makes such commits queue behind one lock. A prototype that sends it after commit gained 36 percent at 16 callers.
- **Finish batches are small.** At most 32 jobs per call and 4 calls at once. River completes up to 5,000 jobs in one statement.

### Finding 4: worker slots sit idle while work is waiting

All verified in `PoolCoordinator.cs` unless marked.

1. **In steady state, batching falls apart (inferred effect).** The claim loop wakes every time one slot frees. With one free slot it does not use the batched claim at all. It sends a Reserve and then a separate Begin: two commands, four trips, for one job (`PoolCoordinator.cs:347-362`, `AttemptDriver.cs:294-350`).
2. **Only one claim is in flight per pool.** Slots that free during a claim wait for it to return.
3. **A slot is held until its finish is confirmed.** It waits in the finish queue and through the finish round trip.
4. **Batching needs exactly one eligible lane.** One queue with two priorities already falls back to one job per claim.
5. **After a "no work" answer the lane is set aside.** Getting it back takes an idle wait of at least 100 ms, a new probe and then a claim. In one case a wake-up is swallowed and the lane waits up to 2 seconds (inferred, untested).
6. **A pressure gate may explain the 23 s tail (inferred).** A control call slower than 1 s, or a finish older than 5 s, stops all refill for at least 3 s (`WorkerEngine.cs:482-508`).

### Finding 5: the outbox relay is limited by its own bookkeeping

Each message costs a claim, a write to record the transport command, the delivery, and a write to mark it delivered. The two writes run one at a time on a single connection, each as its own commit (`PostgresOutboxSource.cs:277-304`). Two commits of about 20 ms each give about 25 messages a second, which is what was measured. Adding relay slots would not help (inferred from the code; matches the measurement).

### Finding 6: memory goes on copies and throw-away trees

- **Producer (verified):** the payload is serialized, then scanned into a full tree of objects, then re-encoded into a canonical form, then copied again into the command envelope, then hashed three times. The argument builder creates 23 arrays even for one job.
- **Worker (verified):** every reply is parsed into a JSON tree. The payload is scanned into a full tree only to validate it, and the tree is thrown away before the real deserialize.

### Finding 7: Redis has one specific fault and two missing techniques

- **Ordinary batches fall into the recovery path (verified mechanism, inferred counts).** A script applies at most 100 operations inline (`INLINE = 100` in `prelude.lua`) and hands the rest to recovery calls. A 100-job bulk add is about 210 operations, so it takes 4 trips. A finish batch of 32 is about 105 operations, just over the line. While one call is in recovery, every other call for the application is told "recovering". This is the likely cause of the "outcome unknown" answers the shoot-out found.
- **No finish-and-fetch-next.** BullMQ's finish script also returns the next job, so a busy worker needs one trip per job.
- **No wake-up on Redis.** Workers poll (spec 22 §1). BullMQ blocks on a marker key.
- **Every payload byte is written to the log twice,** once in the stored intent and once in the job.

### What each library teaches

| Library | What makes it fast | What it gives up |
|---|---|---|
| River | One row per job. Bulk add has a `COPY` fast API with distinct conflict semantics. One fetch fills every free slot. Completions are buffered; 100 is a flush threshold, smaller timer-driven batches are possible, and statements are capped at 5,000. One wake message per queue per 100 ms. | A crash can lose unpersisted completion records, causing reruns; the roughly 250 ms timer is not a maximum loss window under backlog or failures. In the reviewed version, an old completion can be accepted after rescue and a new claim because it does not compare attempt identity. A crashed worker's jobs wait an hour by default. |
| pg-boss | One row per job. Add is a single autocommit statement. Uses workload-specific partial indexes; the pinned 12.38.0 schema has about four applicable indexes for a plain ready job, including its primary key. Its fence costs no writes: claiming bumps a counter and the finish must match it. A hot queue can get its own partition. | No receipts and no history. A crashed worker's jobs wait 15 minutes by default. |
| BullMQ | One script per step. Finish also fetches the next job. All locks are renewed in one call. Concurrent commands can share an AOF flush; one socket is not a requirement for Redis group commit. | Recommends persistence configuration, commonly AOF every second, but supports other profiles. A stable custom job ID deduplicates while that job remains stored; this is not a command receipt. Expired locks can lead to reruns, but finish checks the lock token and rejects missing/mismatched ownership. |
| Laravel queues | Simplicity. The database driver fetches one job at a time; its trip count depends on driver/protocol and needs a trace. | No fence at all: after `retry_after` a second worker takes the job and the first worker's delete removes it from under the second. |
| Horizon | Scales worker processes per queue by "backlog times average run time", moving one process per 3 seconds. | Its dashboard bookkeeping makes it the slowest Redis setup measured. |

## 3. The principles: what each costs, and what I recommend

You said the principles are mostly fixed, and that you would drop some if they cost a lot for little benefit. This table is the input for that decision. Shares are of the writes for one plain job (finding 2).

| Principle | Cost today | Recommendation |
|---|---|---|
| A stale worker's late write is refused (fence) | Under 2% | **Keep.** It is nearly free and it is what River and Laravel lack. |
| Every change is on disk before the caller is told (PostgreSQL) | Every commit waits for durability; concurrent commits can share a flush | **Keep,** and make more jobs share each flush (work area B). Neither River nor pg-boss turns this off. |
| At-least-once delivery, never silently lost | None beyond the above | **Keep.** |
| Fresh database time after taking locks | Negligible | **Keep.** |
| Uniqueness keys | Nothing unless a job has a key | **Keep.** |
| Every command is safe to retry (receipts) | About 25% | **Keep the promise, cut the cost** (C3). Then offer per-queue opt-outs (G2). |
| Each finish is confirmed on disk before the slot is reused | Slot held for a finish round trip | **Keep as the default. Offer a buffered finish per queue** (G1), as River does. A crash then reruns the last moments of finished jobs, which at-least-once already allows. |
| Full per-job history as event rows | About 30% | **Thin it by default** (C2: one "started" row instead of two). **Offer a lean history level per queue** (G3). |
| Generations in their own table | About 12% | **Keep for now.** Folding the first generation into the job row (C7) is the riskiest write-diet step and goes last. |
| A capability and revision check on every call, by locking two rows | Two shared row locks per call | **Keep the synchronization promise.** Replace locks only if C4 proves an equivalent barrier and a material gain. |
| Fair shares decided one admission at a time | No batching in pools with more than one lane | **Relax:** split a batch by the same weights (B5). Shares hold over a batch instead of per job. |
| The producer bounds both waiting callers and their wait time | Up to half of offers refused in P-01 | **Change:** group queued work (B1), with count, byte and wait-time bounds. Preserve explicit overload refusal and command deadlines. |
| One claim in flight per pool, nothing fetched ahead | Slots idle during claims | **Keep "nothing fetched ahead". Review "one claim in flight"** after B3 to B5 are measured. An earlier trial of several claims in flight was slower. |
| A bulk add is all-or-nothing with one receipt, at most 100 jobs | Bulk rate about 3,900/s | **Keep all-or-nothing, raise the cap** with a byte limit (B2). |
| Redis must flush on every write (`appendfsync always`) | BullMQ's own numbers fall 2 to 3 times under it | **Keep as the default. Offer a named "flush each second" profile** (G4) that is clearly labelled as able to lose the last second. |
| Redis stores the whole change as an intent before applying it | Every payload byte logged twice | **Keep the intent, stop embedding the payload in it** (F5). |

Not on the table, as before: unlogged tables, turning off the disk wait on PostgreSQL, sharing one connection between transactions, and using database locks as leases. The reasons are in `perf-plan-draft.md`.

## 4. Targets

These are proposals for you to set. They are for the shared machine and are only directional until the quiet-host run.

| Test | Now | Target | Stretch |
|---|---|---|---|
| Add, 16 callers | 306/s | 1,500/s | 3,000/s |
| Add, 1 caller | 63/s | 120/s | 160/s |
| Bulk add | 3,900/s | 10,000/s | 20,000/s |
| Drain, 16 workers | 188/s | 600/s | 1,000/s |
| Drain, 64 workers | 381/s | 2,000/s | 3,500/s |
| P-01 at 100/s | backlog grows | keeps up, due-to-start p99 under 2 s | |
| P-09 relay | 25/s | 100/s | 300/s |
| Memory per job | about 120 KB | 60 KiB | 40 KiB |
| Redis add, 16 callers | 332/s | ≥90% of paired BullMQ throughput | Match or exceed paired BullMQ |
| Redis drain, 64 workers | 241/s | ≥90% of paired BullMQ throughput | Match or exceed paired BullMQ |

### 4.1 The critical path to large gains

The objective is a several-fold increase in PostgreSQL and Redis throughput under the default durability profile, not a collection of small wins. Section 4 targets stay provisional until the baseline is repeatable. Redis belongs in this round. Do not multiply speculative gains: batching, reduced writes and fewer trips often remove the same bottleneck.

| Milestone | First changes to measure | Evidence needed to continue |
|---|---|---|
| PG-1: remove serialization and idle slots | A1–A3, B3/B4; measure C1 and A4/A5 early | Lower producer wait and worker slot-idle time, fewer commits per completed job, no lease/fairness regression |
| PG-2: amortize durable producer work | B1 with a set-based common path, then B2 | Observed group-size distribution, single-caller latency, fewer statements/WAL bytes per job; no per-item SQL-function loop presented as the final fast path |
| PG-3: reduce the persistent cost | C1, safe C3 compression, C5; C2 only after its event-contract decision | Lower WAL and CPU per job with retained data, not just empty-table throughput |
| RX-1: remove normal-path recovery | A7 with explicit operation/key/byte/time budgets | Zero recovery-path entries for the declared supported normal workload; injected faults must still recover |
| RX-2: reduce scripts per completed job | F2 and F4, then F1 for idle latency | Fewer script invocations and lower AOF bytes per job; bounded renewal/control latency under sustained producer load |
| RX-3: amortize producer and payload cost | F3, then F5 if bytes dominate | More accepted jobs/s with `appendfsync always`, no receipt or recovery regression |

At each milestone, publish the dominant remaining cost and choose the next step from that evidence. If ordinary writes dominate PostgreSQL after batching, advance C rather than increasing connections again. If Lua CPU dominates Redis after A7, reduce script work before widening lanes. B6 is a later experiment: full durable finish batches can deliver gains without the complexity of combining finish and claim.

### 4.2 Measurement and acceptance contract

- Record exact Runwick and competitor commits/versions, database versions, payloads, durability settings, worker/client counts, storage and connection limits. Compare equivalent safety profiles separately from each library's defaults. Pin the upstream sources used; do not treat a moving upstream branch as the benchmarked version.
- Start with at least five paired old/new runs, alternating order after warm-up. Report medians, spread, and p95/p99 latency. Proposed material-regression threshold: 10% throughput loss or 10% tail-latency increase under the same load. If host noise cannot distinguish that threshold, mark the result inconclusive and rerun on a quiet host; correctness must always pass.
- Measure offered, accepted, rejected and completed jobs separately. Throughput gains do not count if they only move work into an ever-growing queue. Report queue depth/slope, oldest queued age, producer wait, finish wait, slot-idle fraction, pool wait, and peak retained memory.
- Run both saturated drains and open-loop arrivals, including P-01, bursts, and low traffic. Sweep producer concurrency 1/8/16/64, workers 16/64, and batch size/in-flight depth independently. Include 1 KiB, 64 KiB and 1 MiB payloads and realistic job durations; large payload tests use smaller byte-bounded batches.
- Use fresh and retained-data fixtures (for example 1 million terminal jobs plus active work), with dashboard reads and retention cleanup running. Exercise multiple queues/priorities, hot/cold lanes, shared scopes, rate limits, uniqueness, retries and several worker processes. Larger batches must not starve cold queues or delay renewal.
- Redis runs include AOF rewrite and restart/recovery; PostgreSQL runs include checkpoint/autovacuum activity. Record storage CPU, I/O, bytes and maintenance separately. Use isolated database instances for per-job WAL/AOF attribution; shared global counters include unrelated work.
- Retain the original P-06 scheduler target (p99 ≤ 13 s), P-01, and P-09 as explicit gates alongside throughput. A faster drain alone is insufficient.

### 4.3 Redis objective: close to or matching BullMQ

**Owner objective, 2026-11-11:** aim to match or exceed BullMQ. Working definition of “close”: at least 90% of its throughput, with p95/p99 enqueue and due-to-start latency no more than 10% worse at equal offered load below saturation. This 10% band is a proposed engineering acceptance threshold, not a measured result or a user-specified number. The desired outcome is parity; the near-parity band identifies remaining work explicitly.

Use paired runs of pinned versions on the same hardware, Redis version, topology, AOF/replication acknowledgement settings, payloads, worker/client counts and workload. Compare single adds, bulk adds, steady arrivals, burst drains, delayed jobs and sustained backlog independently; do not hide a weak case in one aggregate score. Record effective batch sizes. Historical BullMQ figures of 925 adds/s and 1,754 completions/s only illustrate the previous gap; they are not permanent targets or new measurements.

The primary target retains Runwick's default receipts, fencing and durable acknowledgement profile. Matching Redis persistence does not equate all command semantics: disclose history, deduplication/replay, retry and recovery differences. Optional lean-history/buffered-finish/every-second modes get separate results and cannot substitute for the primary target. Repeat comparisons for supported Sentinel and Cluster profiles as R lands; Cluster scaling tests must place comparable work on multiple primaries for both libraries.

Measure accepted adds and **durably settled completions**, not just handlers returning. Require stable backlog under the tested arrival rate, no increased refusal/unknown-outcome rate, and bounded memory/lease-renewal latency. Report CPU and AOF bytes per job as well as latency and throughput. If statistical uncertainty spans the acceptance band, the result is inconclusive. If parity is missed, publish the bottleneck and next measured experiment instead of claiming it based on one favorable workload.

## 5. Ground rules

1. **Measure before and after,** old and new builds taking turns on the same machine, with the load recorded (`eng/bench-compare/ab.sh`).
2. **Every database change is a new function version and a new schema revision.** Upgrades are stop-and-migrate (your decision of 2026-11-02). No function is edited in place.
3. **Every step says which guarantee it touches.** Steps in work areas A to F must preserve guarantees except explicitly reviewed contract changes (including B1, B5 and C2). C1, C3, C4 and F5 require invariant proofs before they can be called behaviour-preserving. Work area G contains optional performance tradeoffs; each needs your yes. R adds required topologies, but any weaker acknowledgement/authority profile also requires an explicit guarantee decision.
4. **Logical work is tested; physical work and timings are measured.** Pin logical row mutations, statements and protocol calls for fixed scenarios in PERF-01. Index maintenance, HOT updates, WAL and actual disk flushes depend on storage state and must be measured, not inferred exactly from row counts.
5. **Full suites after every merge:** Core, Outbox, Postgres, and Redis/SQLite when the provider or shared worker/protocol/serialization code changes.
6. **One writer per file at a time.** The SQL migrations, `PgStore.*.cs`, `PoolCoordinator.cs` and `WorkerEngine.cs` are shared by many steps, so steps that touch them run one after another.
7. **Digest changes need golden vectors.** Any change to how bytes are encoded or hashed must produce identical digests on the existing vectors.

### 5.1 Correctness conditions for the fast paths

These are implementation acceptance criteria, not requests to defer all performance work.

1. **Bound every queue and batch.** Count jobs, bytes, maximum age and in-flight operations. Preserve admission before expensive preparation where possible. Specify cancellation before submission versus cancellation of a wait after submission, expired prepared commands, queue-full refusal, disposal, shutdown and store outage. Group only compatible store/application/transaction contexts. Keep receipts independent for B1; one malformed/conflicting item must not change its neighbours' identities or replay results. A database transaction failure may roll back the group: retry with the original IDs and resolve unknown outcomes, never report uncommitted items as successful.
2. **Preserve historical replay, not just current state.** C3/B2 must enumerate which reply fields are stored or reconstructed and where the immutable source survives until receipt expiry. Test after lease renewal/recovery, job retry/rerun, policy edits and cleanup. The current claim codec includes revision, expiry, policy and fences (`ReceiptCodec.cs:100–138`). The current retention contract allows receipts to outlive jobs (spec 17 §3.2).
3. **Preserve production invariants.** C1 must replace serializer-profile equality enforcement if its composite FK is removed. C3 must not silently remove the decided-receipt invariant. C4 needs a transaction-wide barrier shared with every gate-changing administrative path: pause a command after its check, race the gate change, and verify that no old command crosses the new barrier.
4. **Test mixed batches and failure boundaries.** Faults before mutation, during mutation, after commit/before reply; overlapping command IDs, uniqueness keys and concurrency scopes; one replay beside one new item; cancellation and admission closure while batching. B6/F2 must resolve the original next-job claim after a lost reply and reserve local capacity for it. Global SQL lock order must cover both halves of the operation.
5. **Finish batching has a deadline.** Flush partial batches before lease safety margins, including under low traffic. Do not delay forever while another call is stuck. Buffered finishes need a separate bounded backlog; storage permits are not released merely because the local slot is reused. Preserve renewal/finish exclusion and stop admission when pending settlements threaten ownership.
6. **Redis optimizations keep recovery complete.** The current prelude budgets are `INLINE=100`, `MAX_OPS=1024`, `MAX_KEYS=256`, `MAX_BYTES=4 MiB` (`Lua/prelude.lua:14`). An operation may itself contain many fields, so operation count is not a CPU-time bound. Test payload size, retries/history, keys and worst-case script duration together. A PostgreSQL cap increase does not automatically apply to Redis. Do not split an all-or-nothing public batch into independently committed chunks; reject an unsupported size before mutation or design an equivalent atomic protocol.
7. **Migration and shared-code coverage.** Rehearse upgrades on retained data, receipt-codec compatibility, index rebuild time/disk headroom and interrupted migration recovery. Shared worker/serialization changes run PostgreSQL, Redis and SQLite suites even when no provider file changed. Physical schema experiments keep a recoverable migration path and must not edit released functions.

## 6. Phase 0: confirm before building

Several later steps depend on inferences. Start with lightweight counters, then targeted profiles in disposable fixtures, and write one report. Do not promise this phase takes only a few hours: nested SQL traces and retained-data baselines may take longer.

| # | Check | Confirms |
|---|---|---|
| 0.1 | Count batched claims against single Reserves during a drain and during P-01 (`BatchClaims`, `ReservesSent`, batches sent). | Finding 4.1: batching falls apart in steady state. |
| 0.2 | Count pressure-gate stops during P-01. | Finding 4.6: the 23 s tail. |
| 0.3 | Sample `pg_stat_activity.wait_event` and `pg_locks` during a 16-caller add, on main and on the wake prototype. | That nothing else serializes adds once the wake lock is gone. The benchmark job has no uniqueness key, so key locks should not appear; confirm it. |
| 0.4 | Read `pg_stat_user_tables` (`n_tup_hot_upd`, dead rows) and `pg_stat_wal` over a 20,000-job run. | Which updates are not in-place, and real log bytes per job, to rank work area C. |
| 0.5 | Trace one 100-job bulk add and one 32-job finish on Redis: trips, and how often "recovering" is returned. | Finding 7, first point. |
| 0.6 | Time the relay's source writes and its destination call separately. | Finding 5. |
| 0.7 | Take an allocation profile of one add and one job run with a 1 KiB and a 1 MiB payload. | Finding 6, and the order of work area E. |
| 0.8 | Count how many jobs each finish call carries during a drain. | Whether finish batches are full or mostly ones and twos, which sets the value of B4. |
| 0.9 | Break producer/worker time into serialization, local queue, lane/pool wait, network, SQL/script execution, commit, and idle slots. Record group sizes and in-flight depth. | Whether a proposed throughput gain attacks the dominant cost; add caller latency and RSS/GC to the allocation profile. |
| 0.10 | Inspect nested hot SQL using statement statistics or controlled nested-statement tracing; capture `EXPLAIN (ANALYZE, BUFFERS, WAL)` on representative statements in disposable fixtures. | SQL CPU, scans, locks, WAL and index costs. A top-level function-call plan does not expose its internal statements. Record existing auto-prepare settings before proposing preparation as new work. |
| 0.11 | Trace Redis decision, intent encoding/projection, recovery calls, script duration, AOF growth and client queueing; sweep bounded pipeline depth at `appendfsync always`. | Whether script CPU, doubled payload bytes, recovery serialization or fsync limits throughput. Measure unrelated control/renewal latency during large scripts. |
| 0.12 | Trace P-06 from schedule due time through allowance wait, occurrence materialization and dispatch. | Separates scheduler work from worker due-to-start latency; derive the missing scheduler fix from evidence. |

Housekeeping in the same phase: commit this plan, mark the status table in `perf-plan-draft.md` as finished, and merge the shoot-out report branch (`claude/affectionate-lichterman-448f26`) so its numbers are on main.

## 7. The work

Each step lists what changes, the expected gain, the risk and how we know it is done. "Guarantee: none" means behaviour visible to a user does not change.

### Work area N: .NET 11 / C# 15 migration — deferred

**Latest owner direction, 2026-11-11:** ignore this migration for now and focus on SQLite performance. Retain the current .NET 10/C# 14 build and SDK pin. N is not a prerequisite for Phase 0, optimization, tests or release work in this plan. The earlier .NET 11-only proposal is deferred, not implemented; do not change frameworks, dependencies or benchmark runtimes as part of the SQLite work.

### Work area Q: make SQLite substantially faster (current priority)

**Owner direction, 2026-11-11:** prioritize SQLite performance. The historical drain result is about 36 jobs/s with 16 workers; reproduce it before setting an absolute target. Working experimental goal: a several-fold improvement (start by testing a 3× improvement against a paired baseline), preserving `WAL + synchronous=FULL`, command replay and fencing. This is an aspiration to validate, not an estimate of attainable throughput. Record enqueue latency, durable completion, scheduler lag, fairness and peak memory as well as jobs/s.

**Verified existing architecture:** `SqExecutor` owns one long-lived writer connection/thread and two reader threads by default. It queues writes by strict class priority (renewal → settlement → admission → producer → background). `SqRunner` wraps a driver-owned command in `BEGIN IMMEDIATE … COMMIT`. Existing `ReserveAndBegin`, `FinishMany` and `RenewMany` already share a transaction within each explicit batch; separate calls are not merged. `SqFile` configures WAL verification and `synchronous=FULL`, so enabling WAL, adding connection reuse or claiming batching does not exist would be incorrect recommendations.

`SqTx.Sql` creates commands and callers dispose them after use. `FinishManyBody` still loops through receipts and individual finishes with per-item savepoints, even though the outer transaction is shared. These are measurable SQL/prepare/allocation candidates, not proof that they dominate the 36/s result. No SQLite performance run was executed during this source review.

| # | Work | Evidence / acceptance |
|---|---|---|
| Q0 | **Build a repeatable SQLite cost breakdown.** Use a local filesystem and record disk, SQLite/native library version and pragmas. Split local admission/lane wait, writer-queue wait by class, `BEGIN IMMEDIATE` lock wait, SQL execution, commit, checkpoint and worker idle time. Count statements/job, jobs/transaction and busy retries. | Fresh and retained-history runs, 1/4/16/64 workers and 1/8/16 producers, single/bulk adds and drains, 1 KiB/64 KiB/1 MiB payloads. Compare one process with multiple processes on the same file. Include dashboard/retention activity and actual CPU-bound versus short jobs. A reproducible failing/slow fixture precedes changes. |
| Q1 | **Fill existing batches and remove worker idle gaps.** Apply A2/A3 and measured B3/B4 behavior to SQLite; support bounded producer grouping with independent command receipts. | More jobs per durable commit, including steady load. Prepared caller-owned transactions remain separate. Explicit count/byte/age limits, per-item errors and stable replay identities, and every successful caller acknowledged only after the enclosing commit. A loop issuing separate transactions on the same connection does not share a commit. |
| Q2 | **Reduce SQL and preparation work per job.** Profile the plain dispatch/claim/finish path, eliminate duplicate queries/updates, reuse prepared commands on their owning connection where beneficial, and prototype set-based receipt/lifecycle writes for homogeneous batches. | Count statements/allocations before and after. Commands are connection-confined with parameter reset and invalidation on replacement/migration. Preserve per-item rejection and savepoint behavior or prove an equivalent implementation. Keep staging and injected rollback/commit-loss tests passing. |
| Q3 | **Bound writer occupancy and prevent queue starvation.** Measure strict-priority producer/background delay, long batches, and external transactions holding the write lock. Bound batch execution time through admission sizing; if starvation is observed, design deadline-aware fairness without starving renewals/finishes. | Track oldest wait per class, lease margins and producer refusal. Pending cancellations must not build an unbounded backlog of queued entries. Do not add more writers to the same file or merely widen lanes; do not interrupt a transaction halfway to admit another command. Change spec 20 priority rules explicitly if scheduling policy changes. |
| Q4 | **Profile WAL checkpoints and long readers.** Record WAL growth, checkpoint duration/progress and long read transactions. Compare defaults with a bounded maintenance checkpoint policy only if checkpoints cause tail spikes. | Readers/dashboard queries promptly release snapshots; WAL does not grow without bound under retention/read load. Preserve FULL durability. Do not silently disable automatic checkpointing without a replacement, and do not force blocking/truncating checkpoints on every job. `journal_size_limit` is not a guarantee that active readers cannot retain a larger WAL. |
| Q5 | **SQLite-specific index/write diet.** Use `EXPLAIN QUERY PLAN`, retained-data fixtures and query timings for ready, expired-lease, receipts, dashboard and cleanup paths. Evaluate redundant indexes and per-row history overhead with the same invariant protections as C. | Lower page/WAL writes and writer occupancy without degrading candidate ordering, event cursors, dashboard or retention. PostgreSQL HOT/fillfactor and server-function recipes do not apply to SQLite. Keep event IDs monotonic as required; removing AUTOINCREMENT needs a replacement for the cursor invariant, not just a speed argument. |
| Q6 | **Wake local workers after committed work.** Evaluate an in-process notification hint shared by writers/readers of the same store; retain bounded polling for other processes and missed hints. | No wake-before-commit behavior, failed notification cannot fail an accepted add, bounded idle work, and no claim that a process-local notification provides cross-process delivery. Measure this separately from saturated drain speed. |
| Q7 | **Qualify durable speed and expose bottlenecks.** Re-run SQLite contracts, scheduler/outbox-source cases and representative crash/reopen tests; extend M with writer wait, busy retries, transaction/commit latency and WAL/checkpoint observations. | Paired results under unchanged durability, no lost acknowledged jobs in the declared failure model, no duplicate command effects, unchanged digest vectors and healthy lease margins. Report memory/disk limits and every residual bottleneck. Stop-and-migrate schema changes follow spec 20. |

**Order:** Q0 → the measured Q1/Q2 bottleneck → Q3/Q4/Q5 as evidence warrants; Q6 targets latency. Reuse shared engine improvements rather than fork SQLite worker behavior. Start with normal batches and plain jobs, then cover shared scopes, uniqueness conflicts, retries, catch-up, caller transactions and multiple processes. Do not use an in-memory run or `synchronous=NORMAL/OFF` to claim the durable SQLite target was reached.

**Why one writer is appropriate:** SQLite WAL permits concurrent readers with one writer at a time. FULL adds WAL synchronization at commit, so sharing a transaction can amortize durability work. Microsoft.Data.Sqlite executes its async ADO.NET methods synchronously; Runwick's dedicated threads already address that issue. More worker cores can run handlers, but do not make one file support simultaneous write transactions. Sources: [SQLite WAL](https://www.sqlite.org/wal.html), [synchronous pragma](https://www.sqlite.org/pragma.html#pragma_synchronous), [Microsoft.Data.Sqlite async limitations](https://learn.microsoft.com/en-us/dotnet/standard/data/sqlite/async).

### In-memory provider: useful separate product profile, not a SQLite speed shortcut

**Current state:** there is no public in-memory provider among the current store packages. `SqFile.Parse` and the SQLite registration API explicitly refuse `:memory:`, `Mode=Memory` and shared-cache mode. Migrations use a private in-memory SQLite connection for schema validation, which is not a supported job store. The current SQLite contract is durable file-backed WAL storage; ordinary in-memory SQLite has a different lifetime and journaling model.

**Recommendation for evaluation:** a separately named volatile provider (candidate package `Runwick.InMemory`) for local development, demos, tests and explicitly disposable in-process jobs. The owner asked why it is absent; this is a proposed addition to assess, not an implemented or fully approved provider specification. It should be selectable explicitly, advertise `volatile` in Describe/dashboard, and never impersonate the durable SQLite acknowledgement profile.

Requirements for a useful design:

- Same worker/handler APIs and documented supported scheduling, retry, receipt, uniqueness and management behaviors during the store instance's lifetime. Use a capability matrix; unsupported commands must be explicit, not no-ops.
- No persistence across process exit/crash and no sharing across processes or machines. All queued jobs, schedules, receipts and history disappear when the store is discarded. Durable outbox delivery must not mistake acceptance into volatile memory for durable receipt; reject that pairing unless a separate explicitly volatile workflow is designed.
- Atomic concurrent state transitions and fenced ownership within the instance, bounded capacity/retention, clear store-instance sharing/lifetime rules, cancellation and disposal, and a controllable clock for tests. Creating another service provider must not accidentally reset a supposedly shared store or leak state across tests.
- Compare a native in-memory state engine with a separate SQLite-in-memory adapter before choosing. Native memory avoids SQL overhead but duplicates substantial protocol logic. SQLite reuse reduces that duplication but needs a new profile, deliberate connection lifetime/sharing and concurrency behavior; ordinary `:memory:` connections do not share one database. Neither is obtained by bypassing `SqFile` checks.
- Run provider-contract tests for supported semantics. Memory tests do not replace PostgreSQL/Redis/SQLite tests for locking, persistence, migrations or crash recovery. Publish volatile results separately from durable performance benchmarks.

SQLite documents the connection-bound lifetime and sharing differences in [In-Memory Databases](https://www.sqlite.org/inmemorydb.html). The existing durability requirement explains why turning on in-memory mode inside the current provider is invalid; it does not mean a distinct volatile provider is undesirable or impossible. Keep this design evaluation separate from Q's immediate durable SQLite performance work.

### Work area A: quick wins (no guarantee touched)

| # | Step | Gain | Risk |
|---|---|---|---|
| A1 | **Send the wake message after commit,** as a proper new function version, plus at most one wake per queue per short interval (River's limiter) and no wake for a job that is not yet due. Start from branch `claude/perf-proto-wake`. | Measured +36% on 16-caller adds. | Low. A lost wake only delays pickup to the next poll. Needs a test that a failed wake does not fail the add. |
| A2 | **Use the batched claim for a single free slot too.** The store already accepts a batch of 1 (`PgStore.ReserveAndBegin.cs:16`). | Steady-state claim goes from 4 trips to 2 (verified shape, inferred effect; confirm with 0.1). | Low. |
| A3 | **Fix the idle gaps in the claim loop:** on a wake, clear the set-aside lane and reset the idle back-off; do not swallow a wake because another lane is cooling. | Cuts the due-to-start tail (inferred). | Low to medium. The probe loop must still be bounded when idle. |
| A4 | **Write resource usage in the same update that closes the attempt.** Same for the two heartbeat updates. | One fewer row update per job. | Low. New function version. |
| A5 | **Create finish receipts for the whole batch in one statement** instead of a loop of one function call per job (`0013_set_finish.sql:157-170`). | Fewer statements per finish batch. | Low. |
| A6 | **Stop rebuilding fixed text per call:** the capabilities JSON, the SQL text per function, and the retry and policy JSON per job type (`PgRunner.cs:303-317, 524-534`). | Less CPU and memory per call. | Low. |
| A7 | **Keep normal Redis batches inline:** measure mutation/key/byte counts, then tune `INLINE` together with count and byte batch limits against the script-time bound in spec 22 §4.3. Raising one constant alone is not sufficient. | Likely the largest Redis bulk gain, and it should end the "recovering" storms (inferred; confirm with 0.5). | Medium. Longer scripts block Redis for longer. |

Done when: PERF-01 pins logical counts, paired A/B runs meet the regression rules in §4.2, and suites pass.

### Work area B: share commits and fill slots

| # | Step | Gain | Risk |
|---|---|---|---|
| B1 | **Group concurrent single adds in the producer.** Calls that arrive together travel as one database call that carries many independent `Dispatch/1` commands, each with its own receipt, result and error. This is the pattern `FinishMany` already uses; it is not `DispatchMany`, which is one all-or-nothing command. It needs a new server function. The producer takes "whatever is already waiting" and waits at most 1 to 2 ms more. `LocalAdmission` accounts for queued jobs and retained bytes, with a bounded wait and explicit overload refusal. Preserve capacity control before expensive serialization; do not build an unbounded queue of prepared payloads. | Potentially large reduction in per-job protocol and SQL overhead. The bulk/single ratio is not a gain forecast: bulk has different receipt costs and PostgreSQL already supports group commit. Measure actual group size and service time before predicting a multiplier. | Medium. One item's failure must not fail its neighbours. A caller's cancellation can only stop its wait. Adds made inside the caller's own transaction cannot join. A lone caller must not get slower. |
| B2 | **Raise the bulk cap from 100,** with a byte limit beside the count. First remove the steps that grow with the square of the batch (`= ANY (v_created)` in four statements, `array_position` per keyed item). Compact the receipt only where the original result remains exactly reconstructible through its full replay horizon (§5.1). Try 1,000 first; consider `COPY` into a staging table only if that is not enough. | Bulk add toward 10,000/s (inferred). | Medium. Statement time, lock hold time and receipt size all grow. The export codec also says 1 to 100. |
| B3 | **Keep claims full and promptly refill.** The existing batched path already takes free slots up to 32. Instrument wake/refill scheduling, combine with A2, and sweep the cap; change only the measured source of idle capacity. | Fewer, fuller claims. | Low to medium. Never start more jobs than free slots. The lease deadline of a batch runs from when the batch was sent. |
| B4 | **Bigger, smarter finish batches:** flush when a call returns or when enough are waiting, never send a lone finish while another call is in flight, and raise the cap of 32. | Fewer commits per finished job. | Medium. Bound oldest-finish age; flush a partial batch before its lease safety margin. No lease renewal may run while that job's finish is in flight. |
| B5 | **Batch claims in pools with several lanes.** Split the free slots by the lanes' weights and claim per lane. | Large for multi-queue pools, which claim one job at a time today. | Medium. Changes the fairness rule in spec 05 §8.4 from "per admission" to "per batch". Aging picks need their own rule. |
| B6 | **Finish a batch and claim the next batch in one transaction** (BullMQ's finish-and-fetch-next). | Saves one commit and one trip per cycle on a busy worker (inferred). | Medium. Two receipts and two outcomes in one call; an unknown outcome covers both. Design one global lock order across finish and claim, not two existing functions concatenated. On lost reply, recover the original claim before fetching more; never execute beyond local capacity. |
| B7 | **Revisit the defaults after B1 to B6:** pool size 4, group limit 16, lanes of 4, producer calls 8. | Better out-of-the-box numbers. | More connections per process. Decide from measurements. |

Done when: the 16-caller add, both drains and P-01 are re-measured, and P-01 keeps up at 100/s on this machine.

### Work area C: write less per job (PostgreSQL)

Order these by what check 0.4 shows. Every step is a schema revision.

| # | Step | Gain | Risk |
|---|---|---|---|
| C1 | **Index diet on the jobs table.** Remove `jobs_profile_uq` only with a replacement for its serializer-profile agreement invariant; a primary-key-only foreign key is not equivalent. Fold `jobs_queue_list` and `jobs_name_list` together. Make the dashboard list indexes partial or replace them, since each is rewritten on every state change. Measure the existing partial ready index before reshaping it; `job_id` also supports deterministic candidate order and resume cursors. Removing it requires proving those queries and semantics still work. | Fewer index entries on all three versions of every job row (inferred). | Medium. Dashboard lists and the plan tests (PGX-07) must be rechecked. |
| C2 | **Thinner events by default.** Write one event for "reserved and started" on the batched path instead of two. Evaluate time partitions for retention; an event-ID range alone is not proof that every row has expired. Preserve per-row age and cursor semantics. | One fewer event row and 4 fewer index entries per job. | Medium. The event stream is a versioned contract (spec 08); this needs a schema version note. |
| C3 | **Cheaper receipts.** Compact immutable/redundant fields in claim receipts (about 1 kB per job today), retaining the historical result required by §5.1; IDs and tokens alone are not sufficient. Drop `command_by_id` if the cross-class check can use the primary key. Keep the production "decided" constraint unless an equivalent production invariant replaces it; removing it is not a free optimization. | Smaller and fewer receipt writes. | Medium. Replay decoding must change in step. |
| C4 | **Do the gate check without locking rows.** Prototype a transaction-scoped shared advisory lock with matching exclusive barriers on migration, admission changes and capability activation. A revision read by itself is insufficient: state can change between the read and commit. | Removes two row locks per call and their contention between callers (inferred). | Medium. Migration and capability activation must still shut out old writers. PGX-01 covers this. |
| C5 | **Storage settings.** Tune fill factor only where HOT updates are possible; changing an indexed column or index predicate dependency can prevent HOT, regardless of free page space. Tighter autovacuum on jobs, events and receipts. Evaluate time partitions for events and receipts; drop a partition only when every included record has passed its retention/replay horizon. Account for varying `retain_until`, partition-compatible uniqueness and migration cost. | Less bloat, cheaper cleanup, more in-place updates. | Low for settings; medium for partitioning. |
| C6 | **Keep small payloads in the job row** and leave only large ones in the side table. | One fewer row and index entry per add. | Medium. Retention removes payloads separately today (spec 17). |
| C7 | **Fold the first generation into the job row.** | One fewer insert, update and two fewer foreign-key checks per job. | High. It touches the retry and rerun model. Do it last, and only if C1 to C6 leave a gap. |

Done when: PERF-01 pins logical row mutations, measured WAL and index growth per job fall, and claim/dashboard/cleanup latency passes both fresh and retained-data workloads.

#### C8: conditional structural experiment — a smaller operational dataset

If C1–C5 still miss the target and retained-data runs show that history/index maintenance dominates, prototype separating terminal history or immutable detail from the operational rows. Compare this against the simpler index diet before adopting it. The goal is to keep claims and frequent lifecycle updates cheap, not to add another mandatory table update per transition. Preserve job IDs, rerun/retry semantics, payload retention, receipt reconstruction and dashboard pagination. Measure total writes, archival movement, cleanup, query latency and storage, not only claim speed. This is a high-risk schema experiment and a decision gate, not a prerequisite for PG-1/PG-2. It complements C6/C7; do not stack all three restructurings without separate evidence.


### Work area D: the outbox relay

| # | Step | Gain | Risk |
|---|---|---|---|
| D1 | **Assign the transport command ID inside the claim.** | One fewer write per message. | Low. The ID must still be on disk before the send. |
| D2 | **Mark several messages delivered in one statement,** each still guarded by its own lease token. | Fewer source commits. | Low. |
| D3 | **Widen the source control lane and claim more than one row at a time.** | Removes the single-file bottleneck. | Low. More source connections (2 today). |
| D4 | **Deliver in groups:** a `ReceiveOutboxMany` in the `FinishMany` pattern. | Toward 100/s and beyond (inferred). | Medium. Each delivery keeps its own identity and receipt. |

Done when: P-09 at 100/s passes on this machine.

### Work area E: memory

| # | Step | Gain | Risk |
|---|---|---|---|
| E1 | **Worker: validate the payload without building a tree.** | Probably most of the worker's 8 times on large payloads (inferred). | Low. |
| E2 | **Producer: encode the canonical form into a pooled buffer and hash as it goes,** with no second copy for the envelope. | Most of the producer's 23 times (inferred). | Medium. Digests must stay byte-identical; golden vectors. |
| E3 | **Read replies with a forward reader instead of a JSON tree.** | Less per job in the worker. | Low. |
| E4 | **A one-job fast path for adds:** no array building, no parameter cloning. | Less per add. | Low. |
| E5 | **Slimmer per-attempt objects:** one timer instead of a cancellation source per loop turn, a lazy items dictionary. | Less per job. | Low to medium. Cancellation timing. |

Done when: memory per job is reported for 1 KiB, 64 KiB and 1 MiB payloads, against the target in section 4.

### Work area F: Redis throughput (required)

A7 comes first. Then:

| # | Step | Gain | Risk |
|---|---|---|---|
| F1 | **Wake-up for Redis:** a marker key and a blocking connection, written outside the intent as a hint only. | Lower wait at steady load (158 ms median today). | Low. Polling stays the backstop. |
| F2 | **Finish and claim next in one script,** each with its own receipt. | One trip per job on a busy worker. | Medium. Same as B6. |
| F3 | **Group concurrent single adds into one script** with per-item receipts (the Redis side of B1), bounded by actual keys, operations, bytes and script duration. | Less per-command overhead and an opportunity to share flush work. | Medium. Script size bounds. |
| F4 | **Bounded pipelining and adaptive finish batches.** Sweep in-flight depth on the existing multiplexers before adding sockets; preserve renewal capacity and a maximum finish age. | More commands share each flush. | Low. |
| F5 | **Smaller commands and intents:** profile metadata, payload and event bytes separately. Prototype immutable payload references and a compact event representation. Cache only immutable capability declarations, with reconnect/revision invalidation and server-side checks on every command. | Fewer bytes per flush. | Medium. The recovery rules in spec 22 §4. Payload staging needs durable ordering, immutable identity/digest checks, orphan cleanup, and pinning until every referring intent is resolved. Test crashes before/after staging, intent publication and cleanup. |
| F6 | **Several producer sockets,** only if profiling shows client/socket saturation after F4. Multiple sockets do not make Lua execute in parallel on one Redis server. | Unknown. | Low. |

Done when: Redis workloads satisfy the BullMQ comparison contract in §4.3, and the normal 8-caller bulk test returns no "outcome unknown". Record genuine injected-fault unknown outcomes separately; do not suppress them to pass a benchmark.

#### Redis recovery bottleneck and topology

The current intent guard is application-wide: `prelude.lua` refuses every normal command while `meta.guard` is set. A7 therefore removes a serialization multiplier, not merely a couple of round trips. Keep ordinary supported workloads within the inline/time budgets, and report genuine crash recovery separately from size-triggered recovery.

One Redis server still executes scripts serially. Extra sockets cannot fix server CPU saturation. F reduces work on each primary; work area R adds Sentinel high availability and Redis Cluster support as required capabilities (owner request, 2026-11-11). These tracks are complementary. Cluster distributes work only when keys span slots: the current application hash tag keeps one application on one shard. Supporting a Cluster connection must not be advertised as scaling one hot application across primaries.


### Work area R: Redis high availability and multi-node support (required)

**Owner requirement, 2026-11-11:** Redis must not remain restricted to a directly addressed standalone primary. Support Sentinel-managed primary/replica deployments and Redis Cluster. The old SP-025 exclusions describe the implementation shipped so far, not the desired product boundary. This adds required roadmap work; it does not claim the current driver already supports these topologies.

**Current evidence:** `RxClient.Parse` rejects multiple endpoints and `serviceName`; `RxClient.AdminAsync` assumes a single endpoint; `RxProfile.CheckAsync` requires standalone primary mode. Spec 22 explicitly excludes promotion. Replicas attached to the directly addressed primary are permitted today, but that is not automatic failover support. The reconnect detector compares store identity and the highest locally observed application revision; spec 22 §2.4 explicitly says this cannot detect every lost/divergent history.

| Deployment | Required capability | What it does not imply |
|---|---|---|
| Standalone primary | Keep the existing deployment and acknowledgement profile working | No automatic failover |
| Sentinel + primary/replicas | Discover the primary, reconnect after promotion, revalidate it, and recover or clearly stop unresolved work under the chosen HA profile | Replicas do not increase primary write throughput; Sentinel is not sharding |
| Redis Cluster + replicas | Discover slot owners, route commands, handle resharding and failover, and distribute independent atomic domains across primaries | A single application hash tag still occupies one slot; topology support alone does not distribute that application's writes |

| # | Work | Required evidence |
|---|---|---|
| R0 | **Define the HA acknowledgement and authority contract before changing validation.** Separate persistence on the former primary, replication to peers, survival after promotion and permission for an old worker to write. Specify supported failures, replica acknowledgement policy, promotion eligibility, authority invalidation, and behavior when continuity cannot be established. | A written failure model and testable profile. Do not silently reuse the single-primary profile name or claim no acknowledged job loss from asynchronous replication. Topology support is required; any weakened durability promise needs an explicit decision, not an implicit side effect of enabling Sentinel. |
| R1 | **Sentinel-aware connections and discovery.** Accept multiple Sentinel endpoints and service name; support Sentinel/data-node authentication and TLS, current-primary discovery, work/control/listener reconnection and primary-specific admin checks. | Sentinel startup, failover and repeated reconnect tests. Validate the newly selected primary before admitting work; keep ownership-sensitive reads off replicas. Surface discovery, verification and recovery states in health/Describe. Removing the existing refusal alone is not completion. |
| R2 | **Failover-aware command and lease recovery.** Keep original command IDs/bodies on uncertain writes; stop new admission while topology/authority is unresolved. Specify when local leases are invalidated and when same-command replay is safe on the new history. | Failover before intent creation, mid-projection, after publication/before reply, and during claim/renew/finish. A pending intent must recover or report a defined unavailable/unknown state. Never infer intact history from `PING`, successful connection or a nondecreasing counter alone. Test the isolated old primary and still-running workers as well as the promoted replica. |
| R3 | **Cluster routing and script/key audit.** Support seed endpoints, slot routing, `MOVED`/`ASK`, transient resharding failures, script-cache reloads and node-specific checks. Audit every script's accessed keys and pass the required key arguments explicitly; the current prelude derives keys from `KEYS[1]`, so existing hash tags are not proof of Cluster compatibility. | All atomic command keys in one slot; explicit handling of database-zero restriction, global `rw:store` metadata, per-application metadata, migration, scans, management, retention and notifications across nodes. Exercise slot moves with pending intents and receipt retries. Version any necessary key-layout change. |
| R4 | **Define the sharding boundary and hot-application scaling.** First prove Cluster operation with application-level co-location. Then design explicit partitions within a hot application if required for scale; enumerate shared uniqueness, rates, concurrency scopes, receipts, schedules, outbox identities and ordering that cross a partition. | Distinguish multi-application scale from single-application scale in benchmarks and product claims. Either preserve cross-partition invariants through a designed protocol or reject unsupported combinations explicitly. No silent conversion of application-wide limits into per-shard limits, and no claim of cross-slot atomicity from ordinary Lua. Document layout migration and routing stability for replay. |
| R5 | **Qualify HA and performance together.** Add reproducible Sentinel and Cluster fixtures, deployment docs, topology options and operator diagnostics. Update spec 22, acknowledgement-profile identifiers, configuration validation, history handling and provider contracts as implementations land. | Sentinel fixture: primary, two replicas and three Sentinels. Cluster fixture: three primaries with replicas, with keys placed on distinct primaries. Inject primary loss, partition/heal, replica lag, failback, AOF rewrite, resharding and restart. Reconcile jobs, receipts, occurrences, permits and fences; measure outage/recovery time, duplicate executions, unknown outcomes and any acknowledged loss against the declared profile. |

**Sequence:** start R0/R1 design alongside A7/F; implement Sentinel first, then Cluster routing and application-level distribution, with R4 required before claiming one application's writes scale across primaries. Qualify R2 for each topology, not only Sentinel. Do not postpone all HA work until performance targets are met, and do not count an unsafe reconnect as successful availability.

**Durability experiment:** evaluate replica acknowledgements (`WAIT`) and AOF acknowledgements (`WAITAOF`) as explicit profile mechanisms, not magic consistency switches. `WAITAOF` covers prior writes on its connection: prove ordering and connection/node affinity through multiplexing, reconnection and routing. Check returned acknowledgement counts; a timeout after the mutation is an uncertain outcome, not proof that no write happened. Benchmark acknowledgement batching with its added latency and throughput cost. Neither command by itself establishes a general consensus/old-primary fencing protocol. If the required failure guarantees cannot be met by a proposed profile, record the limitation and redesign or seek a specific guarantee decision; do not mark the topology requirement delivered by weakening it silently.

**Primary references (reviewed 2026-11-11):** [Redis Sentinel](https://redis.io/docs/latest/management/sentinel/) documents possible acknowledged-write loss with asynchronous replication; [Redis replication](https://redis.io/docs/latest/manual/replication/) explains that `WAIT` does not make failover strongly consistent. [WAITAOF](https://redis.io/docs/latest/commands/waitaof/) specifies connection-scoped persistence acknowledgements. [Redis Cluster specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/) describes hash slots, hash tags and redirection. These establish design constraints, not evidence that Runwick has implemented or qualified HA.

### Work area G: optional modes (each needs your decision)

These are optional performance modes. Required topology work in R must separately define its acknowledgement and authority guarantees. Each would be off by default, set per queue or per store, named in the docs with what it can lose, and covered by its own tests.

| # | Mode | What you gain | What you give up when it is on |
|---|---|---|---|
| G1 | **Buffered finish.** The slot is reused at once; finishes are flushed every few tens of milliseconds. | River's main drain advantage. | A worker crash can rerun jobs whose finishes were still in memory. Bound pending finish count/bytes/age, stop refill when the bound is hit, preserve lease handling until settlement, and define shutdown flushing. Remote permits remain held until storage accepts the finish. |
| G2 | **Reduced replay protection (defer).** Do not substitute ordinary active/window uniqueness for a receipt. Any lighter mode needs an explicit deduplication horizon and enough historical finish identity to survive retries and reruns. | Potentially fewer receipt writes; unmeasured. | Active keys clear at terminal state and window keys expire: a lost-reply resend can create another job. Current job state cannot generally reproduce an earlier finish. This weakens command idempotency, not just history retention. |
| G3 | **Lean history.** Only the job's current state and its attempts are kept, with no event rows. | Most of the 30% event cost. | The per-job timeline and the event stream for that queue. |
| G4 | **Redis "flush each second" profile.** | 2 to 3 times, going by BullMQ's own ratio (measured for BullMQ, inferred for Runwick). | Recent acknowledged commands after a crash; roughly one second under normal fsync scheduling, not a universal hard loss bound. Report persistence and replication/failover assumptions separately. |
| G5 | **Fewer lease renewals for short jobs,** with pg-boss's counter fence and a timeout sweep. | Fewer writes for long-running batches. | Faster crash detection. Low priority: renewal already costs one call per 64 leases and jobs under 12 s never renew. |

My suggestion: decide G1 and G3 after PG-1/PG-2/RX-1 and the measured write diet. If saturated finish queues or event writes remain dominant, evaluate those modes promptly rather than waiting for every low-priority schema experiment. If the targets are met without them, they can wait for a user who asks.

### Work area S: scheduler lag (in this round)

**Reviewed 2026-11-11.** The scheduler needs its own fix path; worker throughput improvements alone do not fix occurrence materialization. P-06 measures scheduled instant → durable materialization, not scheduled instant → handler start. The reported p99 is 52 s against 13 s for 1,000 schedules at 100 jobs/s. This review inspected code; it did not rerun P-06 or establish the exact time breakdown.

**Verified structural bottleneck:** `ScheduleBounds.DueReadLimit` is 32. `SchedulerService.LoopAsync` awaits every schedule's materialization serially, then waits the default `PollInterval` of one second even after a full productive page. For 1,000 simultaneous schedules each emitting one occurrence, 32 pages require 31 between-page waits: about 31 s with default jitter, absent wake notifications or an earlier known-due deadline shortening them, before adding database/evaluator time. The configured 100/s token bucket is a ceiling, not the achieved throughput. A separate allowance cannot undo an unconditional page sleep. This predicts a major source of lag but is not a measured attribution of the 52 s.

Other verified limits: planning uses one `EvaluatorSlot`; each schedule sends its own materialization command; one unresolved outcome can occupy the serial loop for the 30 s resolution budget; PostgreSQL scheduler calls share a background lane of two with other background work. Competing scheduler replicas read the same ordered due rows, with revision checks selecting the winner: replica count alone does not imply disjoint work or linear speedup.

| # | Change / experiment | Acceptance and constraints |
|---|---|---|
| S0 | **Reproduce and attribute P-06.** Add timings for due-read, evaluation, allowance wait, database lane wait, materialization/commit, outcome resolution and intentional sleep; count schedules/page, occurrences/command and revision conflicts. | Re-run the existing P-06s fixture; also use a fast controlled-clock scheduler-loop fixture with more than 32 due schedules. The latter must exercise `LoopAsync`, not just the pure planner. Establish a failing signal before implementation. |
| S1 | **Drain productive due pages without the one-second poll gap.** Fetch the next page promptly while backlog remains; retain the shared token bucket as the output-rate limiter. Apply idle backoff only when no useful progress is available, and bounded retry backoff on failure. | Test 1,000 simultaneously due schedules with unchanged 100/s allowance. Bound per-turn work and observe cancellation; no tight loop on a full page of incompatible, conflicting or otherwise unproductive schedules. Provide a bounded scan/cursor strategy for reaching later eligible rows without skipping them permanently. Preserve per-schedule catch-up pacing (currently ≥250 ms), distinct from the global page sleep. |
| S2 | **Overlap independent schedules' materialization I/O if serial commits remain dominant.** Keep planning under the existing bounded evaluator initially; use a small bounded pending set for independent schedules, starting with a measured 1/2/4 concurrency sweep. | One pending identity/plan per schedule, original IDs through unknown-outcome resolution, thread-safe shared allowance, bounded bytes and shutdown draining. One slow schedule must not stop all unrelated schedules. Respect existing background-lane capacity and measure cleanup contention before widening it. Do not turn the local allowance into one 100/s bucket per task. |
| S3 | **Group independent materialization commands if commit/statement cost still prevents the target.** A new provider operation can carry several schedule plans, each with its own result, revisions and receipt, while sharing database work. | PostgreSQL: deterministic locks across schedules, keys and scopes, with per-item rejection semantics and rollback/retry rules. Redis: normal-path intent/key/byte/time budgets from A7. Preserve atomic occurrence/job/cursor publication per schedule, no duplicate occurrence after lost replies, and existing per-plan limits. This is different from increasing catch-up occurrences for one schedule. |
| S4 | **Use the next durable due instant to bound idle waits.** On an empty due read, consider returning the earliest eligible `scan_after_at`; combine that deadline with edit notifications and a bounded poll fallback. | First-run schedules and schedules last touched by another replica must not depend on a 15 s idle cap. Test lost/reconnected notifications, earlier edits, pause/resume, restart and clock skew. Map store time to local waiting without adding materialization latency to the next deadline (`NoteNextScan` currently maps from the old cutoff when the command completes). A full cache of all schedules is not required. |
| S5 | **Cache immutable compiled recurrence definitions if evaluation profiles show meaningful CPU cost.** Key by definition identity/revision, evaluator profile and TZDB identity; bound the cache. | Current `ScheduleRecurrence.For` compiles a stored cron definition for each plan. Cache only immutable compilation, never the mutable budget/cursor or the closure carrying a per-plan evaluation budget. Keep DST, misfire and artifact checks. Increase evaluator parallelism only if CPU remains limiting and spec 12's single-slot containment is deliberately revised. |
| S6 | **Measure replica contention before introducing distributed ownership.** Run 1/2/4 schedulers and count duplicate planning, rejected revisions and useful materializations. | Preserve existing revision-based correctness first. Consider partitioned acquisition/short claims only if contention is material; a new ownership protocol needs crash recovery, rebalance and abandoned-claim tests. The allowance is currently per scheduler/store group, not application-wide: compare replica throughput at equal aggregate allowance too. |

**Order:** S0/S1 join the first performance milestone; S4 follows the idle-latency evidence. S2 and then S3 are conditional on the remaining cost. S5/S6 are later experiments, not prerequisites. Raising `JobsPerSecond`, adding worker slots or changing misfire policy is not a fix for the current P-06 failure. A lower polling interval can be an experiment, but should not become a global idle polling tax.

**Done when:** P-06s p99 ≤13 s at the existing 1,000-schedule/100-s allowance, including censored/unmaterialized occurrences; sustained minute-boundary bursts do not accumulate backlog. Preserve catch-up episode limits and pacing, no duplicate occurrence, no cursor rewind, pause/edit races, DST gap/fold behavior, bounded memory, stop/admission behavior, and fairness between catching-up and newly due schedules. Run the shared scheduler contracts on PostgreSQL, Redis and SQLite. Amend specs 12/13 and defaults documentation with the implemented changes; this plan itself does not amend them.

The [TickerQ / Quartz.NET review](../reports/scheduler-upstream-review.md) supplies source-backed design comparisons. Borrow their scheduling/execution separation, due-time wakeups and bounded acquisition patterns where appropriate; do not replace Runwick's durable materialization/receipt model with in-memory timers.

### Work area M: store stats, health and details in the management dashboard (required)

**Owner requirement, 2026-11-11:** give operators a dedicated **Store** page, linked from the overview and settings. Support common information across providers and provider-specific details, including the Sentinel/Cluster work in R. This is part of the roadmap, not a claim that the page exists today.

**Existing foundation, verified:** `pages/Settings.jsx` displays provider, schema revision and persistence profile from `ManagementSession.GetSettingsAsync`. `IRunwickHealth` exposes cached liveness/per-role readiness; its health service refreshes on the background lane. `ITelemetrySampleStore` provides bounded queue/scheduler samples, and the Redis profile checker already reads server/version/persistence/replica facts. These are reusable inputs, not a complete store-statistics API. Local role health is not a cluster-wide health assessment.

| View | Required information |
|---|---|
| Overview / health | Reachability, admission readiness, persistence-profile verification, migration/recovery state, readable reason, last successful observation and data age. Separate application health, this host's connection health and provider topology health. Unknown/stale/inaccessible must not appear healthy or as zero. |
| Throughput and latency | Accepted adds/s, durable completions/s, failures/timeouts/unknown outcomes, latency percentiles, batch sizes, local lane/pool wait and scheduler materialization lag. Show backlog/oldest due age alongside throughput. Label process-local versus aggregated application metrics. |
| Store details | Provider and server version, logical store identity, schema/layout revision, supported capabilities, acknowledgement/persistence profile and verification status, admission/migration mode, operational limits and safe node labels. |
| Redis stats / topology | Memory used/limit and fragmentation where exposed, connections, server-wide operations/s, evictions, AOF size/write/rewrite status, primary/replica roles, replication offsets/lag indicators, pending intent/recovery observations, Sentinel-discovered primary or Cluster slot coverage/node states, failover/disconnection events when observed. Server operations/s is not Runwick jobs/s; replication offsets are not automatically a time-lag or durability proof. |
| PostgreSQL stats | Runwick client-pool use/waits, database/session counts, relevant lock waits, database and Runwick relation sizes, dead tuples/autovacuum activity, WAL generation and replication state/lag where permissions and server version allow. Distinguish server/database-wide counters from application-specific work. |
| SQLite details | Database/WAL sizes where accessible, journal mode, busy/locked errors and write contention. No fabricated replication/node metrics; unsupported fields are explicitly unavailable. |
| Recent events / trends | Bounded short history of observations, connection loss/recovery, profile mismatch, failover and recovery progress. Timestamp and identify observer/source; distinguish inferred state changes from authoritative provider events. Link to existing telemetry for long-term history. |

| # | Delivery | Acceptance |
|---|---|---|
| M1 | **Common read-only snapshot API and Store page.** Define typed provider capabilities, common health/details, timestamps, metric scope and partial-result statuses. Reuse cached health and available descriptor/profile facts. | Authorize the page/API; distinguish local/app/store/node scope. Render healthy, degraded, unavailable, stale, unsupported and permission-denied fields. A store outage must not prevent the dashboard from showing a bounded last-known snapshot with an explicit stale indicator. |
| M2 | **Bounded collectors and provider details.** Sample away from job hot paths, with per-provider timeouts, cancellation, backoff and a shared cache. Start with roughly 10 s refresh and measure overhead; topology discovery can be event-driven with bounded reconciliation. | Each browser reads snapshots rather than triggering full database scans or per-node polling. No Redis `KEYS`, unbounded keyspace scans or expensive exact row counts for a dashboard refresh. Narrow optional permissions; missing admin/statistics permissions degrade those fields without granting broader rights or changing the store's existing admission-verification rules. |
| M3 | **Trends and topology views.** Add bounded in-memory observation history first (cleared/labeled after restart); use the existing telemetry export for long-term retention. Integrate Sentinel and Cluster facts as R ships. | Counter-reset/restart handling, defined sample windows and percentile aggregation; do not average independent p99 values. Node/global metrics and cross-application details require appropriate operator authorization. Never expose credentials, raw connection strings or unfiltered provider output. |
| M4 | **Qualify correctness and cost.** Test partial permissions, unavailable nodes, stale caches, replica promotion, slot migration, profile mismatch, store outage and multiple management hosts. | With dashboard polling and collectors active, paired performance runs should stay within a proposed 5% throughput/p99 regression budget; inconclusive noisy results need quiet-host measurement. No added per-job writes for collecting dashboard samples. Keep renewal/settlement capacity protected. |

**Sequence:** ship M1/common health and Redis/PostgreSQL basics early, using existing telemetry; add M2/M3 topology details alongside R. Update management/observability/dashboard specs (16/18/21) when implementing. The existing generic 503 page says “Your data is safe”; replace this unverified assurance with reachability/status wording as part of the health work—connection failure alone cannot establish data integrity, particularly during failover.

### Work area H: prove it

| # | Step |
|---|---|
| H1 | After each work area: the A/B, the three-way comparison, and the full suites. |
| H2 | After A to D: rerun the G-1 and G-2 workloads and the SP-029 short profile. |
| H3 | At the end: the SP-029 full profile and the 24-hour soak on a quiet host. This is the same run the release gate still needs. |
| H4 | Before changing the affected batching/ownership paths, close the known test gaps: no test stops admission while batching is on, and the tests that fail under load (CAP-10, WRK-08, the store-outage test, TIME-04, TIME-09, TXC-05, MODE-02). |

## 8. Order

1. **SQLite focus: Q0 and Phase 0 on the current .NET 10 toolchain.** Establish the SQLite bottleneck and baseline, then prioritize Q1/Q2 according to measurements. N is deferred. Previously required PostgreSQL/Redis work remains on the roadmap.
2. **Work area A plus S0/S1.** A1 is already measured; reproduce the scheduler pacing problem and remove the measured backlog delay early.
3. **Work area B:** B1 and B3/B4 first, B2 when bulk CPU/encoding is measured, then B5/B6 as justified by the milestone results. B1 and B2 change the add function; B3 to B6 change the claim loop and finish path, so those go one after another.
4. **Work area D** can run beside B. It touches the outbox packages only, until D4.
5. **Work area E** can run beside B and C. It touches serialization and reply reading.
6. **Work area C:** start measured index and write reductions (C1, A4/A5, safe C3 portions) alongside the early PostgreSQL milestones, with serialized edits to shared files. Do not postpone the write diet until every B item is done. C4, C6–C8 remain evidence-gated.
7. **Work area F starts immediately after A7**, alongside the PostgreSQL track. **R0/R1 start alongside it:** Sentinel and Cluster are required roadmap capabilities, not excluded topologies. R follows its own staged qualification and may touch shared acknowledgement/profile contracts.
8. **Work area G,** only on your decision, after A to C are measured.
9. **Work area M starts with the first instrumentation milestones:** common store health/details and Redis/PostgreSQL stats, then topology alongside R. **H runs throughout**, including dashboard-on performance checks; H3 is last.

## 9. What this plan does not cover

- **SQLite distributed sharding / network-file deployments.** Q targets the existing local-file provider. Multiple writers to one file do not create independent write capacity; partitioning across files changes shared-scope and transaction semantics and is outside the first optimization steps.
- **A wholesale scheduler replacement.** Work area S covers the known lag with a staged fix path; replacing the evaluator, misfire model or ownership protocol is not required for the first fix.
- **Release-gate leftovers that are not speed:** the Windows and Mac checks, your G-5 decision, the NuGet name check. They are listed in `docs/reports/sp-030.md`.
- **The hosted-store track** (SP-031, SP-033, SP-034).

## 10. Things we still choose not to do

| Idea | Why not |
|---|---|
| Unlogged job tables | A crash empties them. Neither River nor pg-boss uses them for jobs. |
| Turning off the disk wait on PostgreSQL | A crash could lose a change the caller was told had succeeded. |
| River's rescue with no fence | A slow job would run twice and both finishes would be accepted. |
| Laravel's `retry_after` in place of leases | Same problem, and the first worker can delete the job from under the second. |
| Replacing recorded leases with expiring Redis lock keys | Token checks can fence queue transitions (BullMQ does this). TTL expiry alone does not preserve Runwick's recorded recovery, receipt and replay contract; replacing the lease model is outside this optimization round. |
| Sending commit in the same trip as the add | Built and measured in round one: no gain on this machine. Retest only on a quiet host. |
| Several claims in flight per pool | Tried in round one: slower on top of the set-based claim, and one shutdown test failed once. Revisit only after B3 to B5. |

## 11. Decisions that are yours

1. **Redis parity with BullMQ is the goal** (owner request, 2026-11-11); §4.3 proposes a measurable near-parity band. PostgreSQL targets remain provisional, with River default/tuned comparisons reported separately.
2. **Fair shares per batch instead of per admission (B5).** This is a spec change (05 §8.4).
3. **Producer batching and revised queue bounds (B1).** Waiting already exists; any changed wait/count/byte defaults must be documented (06 §"local capacity").
4. **Thinner events by default (C2).** One "started" event instead of two changes what the dashboard timeline and the event stream show.
5. **Work area G.** Which optional modes you want at all, and whether any should be on by default.
6. **The bulk cap (B2).** 1,000 with a byte limit, or straight to `COPY` and several thousand.
7. **Redis performance and multi-node support are required** (owner request, 2026-11-11). Sentinel and Cluster are no longer exclusion decisions. R0 must make the remaining acknowledgement, failover and sharding-boundary choices concrete; no weaker durability is implicitly approved.
8. **A quiet machine.** Phase 0 and the A/B runs work on this machine. The final numbers and the release gate do not.
9. **.NET 11 is deferred** by the latest owner direction. Focus on SQLite speed on the current toolchain; do not make the runtime migration a dependency.

## 12. Questions for the reviewer

1. **Is the batching-falls-apart reading right?** (Finding 4.1, `PoolCoordinator.cs:347-362`.) If the loop already batches in steady state, A2 gains nothing and B3 moves up.
2. **Can B1 keep every per-command promise?** In particular: a resend of one grouped add after an unknown outcome, and one item's conflict or validation failure beside successful neighbours.
3. **Can C4 replace the gate barrier safely and measurably?** A plain revision read is insufficient; the existing row locks synchronize in-flight calls with gate changes.
4. **Does B5 break an aging or starvation rule** that the per-admission design was protecting?
5. **Is the relay arithmetic right?** (Finding 5.) If the destination call is the slow part, D4 moves ahead of D1 to D3.
6. **Are the "about 30, 25, 12 percent" shares the right order?** They are row and index counts, not measured log bytes.
7. **Is anything in section 3 marked "keep" that costs more than it looks, or marked "relax" that a user depends on?**
8. **Is the order in section 8 right,** or should the write diet (C) come before commit sharing (B)?
9. **What is missing?** SQLite is now a first-priority work area (Q); scheduler diagnosis, a staged fix path (S), and its acceptance gate are now included.

## 13. What has not been verified

- Nothing in the four studies was run. Row, index and operation counts are read from code.
- Whether updates are in-place, how payloads are stored on disk, and lock contention on the gate rows are PostgreSQL reasoning, not measurements.
- The Redis operation counts, the per-socket read limit and the script CPU cost are unmeasured.
- Five claims the plan leans on were rechecked directly in the code after the studies: the batched-claim condition (`PoolCoordinator.cs:347-362`), the bulk cap of 100 (`PayloadBounds.cs:16`), Redis `INLINE = 100` and `MAX_KEYS = 256` (`prelude.lua:14`), the finish lane of 4 (`WorkerSettings.cs:30`; one study had read 2, which was wrong) and resource usage on by default (`Builders.cs:460`). The other file and line references come from the studies and were not rechecked.
- The per-job wrappers `reserve_v2`, `begin_v2` and `finish_v2`, unreviewed SQLite paths beyond the targeted Q review, Horizon's tags, and Laravel's batches and chains were not read.
- Expected gains marked "inferred" are estimates. Only A1 has a measurement behind it.


## 14. Second-review evidence and upstream reading

The [upstream review](../reports/perf-round-2-upstream-review.md) records primary-source links, version caveats and which techniques are transferable from River, pg-boss, BullMQ and Laravel. These are design inputs, not measurements of a new Runwick implementation.

Local checks on 2026-11-11: `PoolCoordinator.BatchTickets` takes all available tickets up to the cap already, but returns no batch for one ticket, aging selections or multiple eligible lanes; B3 must distinguish an existing behavior from its proposed scheduling/cap changes. `PgDataSources` already enables automatic statement preparation (64 statements, minimum two uses); increasing preparation is not an unimplemented quick win. `JobDispatcher` acquires local admission before preparing ordinary payloads; preserve that useful memory bound. Redis has separate work/control multiplexers and per-lane limits already; benchmark their queueing rather than assuming a single global client socket.

Additional primary references:

- [PostgreSQL HOT](https://www.postgresql.org/docs/16/storage-hot.html): free page space alone does not make updates HOT when indexed columns change. Partial-index predicates must be considered when designing the index diet.
- [PostgreSQL EXPLAIN](https://www.postgresql.org/docs/18/sql-explain.html): use buffer/WAL instrumentation on controlled workloads; `ANALYZE` executes the statement and instrumentation has overhead. Select options supported by the tested server version.
- [Redis persistence](https://redis.io/docs/latest/management/persistence/): `appendfsync always` supports group commit across concurrent clients/pipelines. Keep this profile for the primary comparison; report relaxed persistence separately.

No new throughput measurements were produced by this review. The new milestones and structural experiments are hypotheses to validate, not promised speedups.


## 15. Multicore use and scaling limits

**Yes, Runwick can execute jobs on multiple cores.** `AttemptDriver.ExecuteAsync` starts each admitted handler pipeline with `Task.Run` on the .NET thread pool. Independent CPU-bound jobs can run in parallel; asynchronous I/O-bound jobs can overlap without occupying a thread while waiting. Runwick does not automatically parallelize the internal work of one handler.

The default pool target is **4 concurrent jobs**, not the number of CPU cores (`WorkerSettings.DefaultPoolTarget`). `PoolBuilder.FixedConcurrency(n)` and `AutoConcurrency(min, max)` set pool capacity; the latter remains bounded by configured limits. Effective execution is also constrained by group/scoped limits, rates and storage admission. More configured slots are not a promise of more CPU throughput, and more cores do not automatically increase those limits.

| Component | Parallelism today | Important limit |
|---|---|---|
| Job handlers | Multiple thread-pool tasks across cores, subject to capacity | CPU, blocking handlers, configured slots, group/scoped limits and rates |
| Worker coordination | Concurrent supervised loops/pools, asynchronous storage calls | One claim in flight per pool, bounded storage lanes and finish settlement |
| Scheduler | Separate background task, one evaluator and sequential materializations | S1 page sleeps and serial I/O; extra cores alone cannot remove those waits |
| PostgreSQL | Concurrent client connections/server sessions can service independent work | Shared locks, WAL/storage throughput and Runwick lane limits; no promise that a single mutation uses parallel query |
| Redis | Concurrent clients and pipelined requests; R plans Cluster distribution across primaries | Lua scripts execute serially per server. One application currently hashes to one slot; R4 addresses partition design before claiming single-application scale |

Add a scaling experiment to H: CPU quotas of 1/2/4/8 cores with concurrency varied independently, comparing real CPU-bound and async I/O-bound handlers. Measure actual CPU use, .NET thread-pool queue length, GC pauses, admission/finish waits and storage utilization. Include default and tuned pool settings, keeping database resources fixed for client scaling and then varying server resources separately. Report speedup and efficiency relative to one core; existing 16/64-worker drain comparisons are concurrency measurements, not proof of multicore scaling.

Use these results for B7's defaults. Do not blindly set every pool to `Environment.ProcessorCount`: several pools share the same cores, and I/O workloads have different needs. Blocking user handlers can also delay coordination on the shared thread pool; distinguish that from database bottlenecks before adding threads.
