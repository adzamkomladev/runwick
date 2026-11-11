# Round-two performance plan: upstream review

Reviewed 2026-11-11. Source review, not a benchmark. River v0.50.0 and pg-boss 12.38.0 match the checked-in comparison manifests. BullMQ `master`, Laravel `12.x`, and official database documentation are current references, not claims about the shoot-out's exact dependency versions. The findings are incorporated into `docs/plans/perf-plan-2.md`.

## Prioritized recommendations

1. **Keep A1–A5 and fuller claim/finish batches ahead of architectural changes.** Measure effective jobs per statement, slot idle time, transaction latency and WAL bytes together. The upstream pattern is set-based transitions and less work per job; trips remain relevant even if they no longer explain the entire throughput gap.
2. **Measure existing group commit before promising B1's 5–10× gain.** Concurrent independent transactions already can share a flush. Producer grouping still amortizes SQL, protocol, receipt and transaction overhead, but the bulk/single throughput ratio is not a prediction of grouping's benefit. Keep per-item errors, cancellation and retry semantics in the experiment.
3. **Treat COPY as a transport option after removing per-item server loops.** Compare larger set-based inserts with COPY-to-staging under identical receipt, uniqueness and payload semantics. Do not compare an all-or-nothing COPY load directly with independent retryable commands without describing the difference.
4. **Test indexes against actual ordering and delayed backlogs.** Include mostly-future jobs, mixed priorities, retained history, concurrent claims and vacuum pressure. Preserve Runwick's tie-breaking/fairness rules unless explicitly changing the contract. Move cheap, evidenced index reductions forward if WAL and buffer measurements support them.
5. **On Redis, first keep normal batches out of recovery and measure script duration; then wakeups and finish/fetch.** Bound work by bytes and execution time as well as command count. Finish/fetch must reserve capacity before claiming the next job and retain replayable outcomes for both halves. More sockets and weaker persistence should remain experiments after this work.

These are engineering recommendations inferred from the evidence below, not measured gains for Runwick.

## Corrections and evidence

### PostgreSQL transactions do not each require a separate physical flush

Finding 3 and the principle table conflate a durable commit wait with one fsync per transaction. PostgreSQL explicitly supports group commit, including with default `commit_delay = 0`: concurrent commits can be covered by one WAL flush. Add commit counts and WAL write/sync counts/timing to Phase 0; use statistics available in the deployed PostgreSQL version and record `wal_sync_method`. Do not change durability to test this. [PostgreSQL WAL configuration](https://www.postgresql.org/docs/current/wal-configuration.html)

Finding 1 is also too categorical: River's faster insertion despite more protocol operations shows that round trips alone do not explain the gap; it does not establish that further trip reduction is useless. A2, B6 and F2 themselves depend on reducing trips.

### River: distinguish the COPY API, batching policy and crash window

`InsertManyFast` uses COPY, returns a count and aborts the operation on a unique conflict; normal `InsertMany` has different conflict handling. The 5,000-job chunk is a choice in this repository's `eng/bench-compare/river/main.go:121`, not a universal River insertion limit. COPY is useful evidence for B2, but Runwick still needs its command receipts and replay semantics. [River v0.50.0 client API](https://github.com/riverqueue/river/blob/v0.50.0/client.go#L2191), [pgx COPY implementation](https://github.com/riverqueue/river/blob/v0.50.0/riverdriver/riverpgxv5/river_pgx_v5_driver.go#L487)

The completer's 100-job threshold is not a minimum batch size. Small batches flush on the periodic path; maximum statement size is 5,000. The approximately 250 ms interval is a scheduling policy, **not a maximum unpersisted-completion window**: database delay, backlog and retries can extend it. Failed batches are requeued and backlog pressure can block more completions. G1 needs bounded outstanding completions, renewal and shutdown behavior, not just a timer. [River batch completer](https://github.com/riverqueue/river/blob/v0.50.0/internal/jobcompleter/job_completer.go#L341)

For this pinned River version, completion checks state and ID, not an expected attempt identity. Therefore the plan's missing-fence concern is supported specifically when the old job has been rescued and is running again; it should not imply every late completion in every state is accepted. [River completion SQL](https://github.com/riverqueue/river/blob/v0.50.0/riverdriver/riverpgxv5/internal/dbsqlc/river_job.sql#L640)

### pg-boss: copy the query/index relationship, not an isolated index recipe

Its ready index is `(name, priority DESC, created_on, start_after)` with a ready/unblocked predicate. The trailing scheduling column allows due-time filtering within the index; a covering column is not equivalent when row locking requires heap access. Random UUID is omitted together with its ORDER BY term. This is evidence for workload-specific index design, not permission to remove Runwick's required tie-breaker.

The schema also has live-monitoring and retention indexes. “A plain job enters 3” needs an exact version and schema recount; the pinned source includes the primary key plus ready, live-monitoring and creation-time indexes for an ordinary ready job. A single broad library count is unsafe.

Attempt fencing is cheap when settling with the fetched attempt identity, but the source documents a `restore()` limitation: clearing `started_on` can allow identity reuse on the next claim. Do not substitute this counter blindly for Runwick's generation/lease model. [pg-boss 12.38.0 SQL and indexes](https://github.com/timgit/pg-boss/blob/12.38.0/src/plans.ts)

### BullMQ and Redis: efficient execution is compatible with ownership checks

BullMQ's finish script can fetch the next job in the same call when requested, avoiding a separate busy-path fetch. It also emits events and maintains other structures: “one script” is not “one write.” Its ordinary lock removal checks the token and rejects a missing or mismatched lock. Thus the plan's categorical claim that expiring Redis keys cannot be fenced is incorrect. This protects queue transitions, not arbitrary external handler side effects, and does not establish Runwick-compatible command replay. [Finish script](https://github.com/taskforcesh/bullmq/blob/master/src/commands/moveToFinished-14.lua), [lock validation](https://github.com/taskforcesh/bullmq/blob/master/src/commands/includes/removeLock.lua)

`appendfsync always` can group commands from **multiple clients or a pipeline** before one fsync and replies. Sharing a socket is not the fundamental requirement. A1/F3/F4 measurements should distinguish application grouping, pipelining and actual server flush grouping. Redis documents potential loss with `everysec`; the plan's 2–3× number must remain a benchmark-specific observation, not a Redis constant. [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)

BullMQ recommends configuring persistence and usually one-second AOF syncing; it does not require that setting. Its production guidance also requires `noeviction`. Therefore say “recommended deployment profile,” not “assumes once-a-second flushing.” Benchmark durability settings must be reported explicitly. [BullMQ production guidance](https://docs.bullmq.io/guide/going-to-production)

A retry after a lost add reply is not unconditionally a duplicate: supplying a stable custom job ID prevents adding the same retained job again. Removing that job removes this protection. This is weaker and different from Runwick receipts, but the comparison should acknowledge it. [BullMQ job IDs](https://docs.bullmq.io/guide/jobs/job-ids)

### Laravel: useful simplicity baseline, not a transferable lease design

The database driver reserves one job within a transaction, updates its attempts/reservation timestamp, and deletes a reserved job by ID under a row lock. The delete does not test an ownership token, supporting the stale-delete concern. “Eight trips” is driver/protocol dependent and needs a trace rather than a universal Laravel label. Retain Runwick's fence while reducing its SQL work. [Laravel 12.x DatabaseQueue](https://github.com/laravel/framework/blob/12.x/src/Illuminate/Queue/DatabaseQueue.php#L285)

## Measurement implications

Report enqueue acceptance, handler return and durable finish as separate timestamps. Match payload, uniqueness, retention, history, crash recovery and persistence settings before calling libraries comparable. Measure steady low arrival rates as well as saturated drains: large batches that help throughput can worsen latency when work is sparse. Upstream source supports these experiment designs; it does not validate the plan's percentage write-cost estimates or predicted multipliers.
