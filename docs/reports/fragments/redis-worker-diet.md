## Redis worker path: where the time goes and what was cut (work area F follow-up)

Written 2026-11-15. Informational. Every timing below was taken on the shared host while other agents' suites and the SP-029 matrix were running (load average 10 to 22 on 8 cores). Runs were alternated inside each round and held the shared bench lock, so each trio saw the same neighbours, but the numbers are still noisy. Script CPU numbers use `os.clock` (the script's own CPU time), so host load does not distort them.

### Short answer

The worker-side Lua scripts now cost about 40% less CPU per job, and drained throughput rose from 136 to 180 jobs/s (1.32×) on the host disk and from 155 to 248 jobs/s (1.6×) with the log kept in memory. That is not close to the targets. BullMQ drained 697 and 843 jobs/s in the same rounds, so Runwick is at 0.26× and 0.29× of it. The ≥90% throughput target is **not met**, and the ≤10% p95/p99 target is **not met** (steady 500 and delayed 100 are both still far off).

Closing the rest means making a drained job cost roughly five times less in Redis (about 1.1 ms now against about 0.2 ms for BullMQ). That needs a protocol change to what each job writes, not more script tuning. Details are in "What is left".

### Where the time went (one drained job, claim plus finish)

Script CPU per job, microseconds, `profile_worker.py`, 40 calls per size, median:

| Batch size | Claim before | Claim after | Finish before | Finish after |
|---|---|---|---|---|
| 1 | 1,953 | 1,154 | 1,156 | 837 |
| 8 | 1,244 | 696 | 730 | 421 |
| 16 | 1,320 | 591 | 806 | 446 |

At batch 8 a job costs 1,974 µs before and 1,117 µs after (43% less). Redis's own time per script call, from `INFO commandstats`, went from 14,245 µs to 6,256 µs per call over a whole drain (this includes the bulk insert calls, the read-only probes and the AOF write).

Where the claim cost sat before (single claim, µs): reading the application and deciding about 340, events 200, building the write list 280, packing the stored intent 430, writing the guard 40, applying the writes 270. Encoding the three copies of the result and sorting object keys with a byte-by-byte comparator written in Lua sat inside those steps; the changes below target them, and the after-numbers are the measure.

### What was changed (no stored data or reply changes)

All of it is in `src/Runwick.Redis/Lua` and was checked with a differential run (`diffcheck.py`): the same fixed dispatch, claim, finish-many and finish sequence goes through the old and the new scripts and every stored key is compared (36 keys, timestamps and the time-derived part of attempt IDs masked). Result: 0 differences. Replies and receipts are unchanged.

1. **Encode each result once.** The receipt, the stored intent and the final reply all carried the same JSON, which was built three times. It is now built once and reused (`U.rjson`, and `finish_many.lua`).
2. **Native byte-order sort when it is provably the same.** Lua's `<` on strings follows the server's locale, which is why the old code used its own loop. The prelude now checks 13 byte pairs that are in byte order in the C locale and out of order in the usual other collations. If all hold it uses the built-in comparison, otherwise the old loop. The probe costs a few microseconds per script call.
3. **A quicker JSON string writer** (no `gsub` when nothing needs escaping) and a shorter path in the encoder.
4. **Event rows written directly** in key order, instead of building a table and sorting its keys for every event.
5. **Begin reuses the reserved job inside `ReserveAndBegin`**, instead of reading the job hash again after Reserve just built it.
6. **Wake hints.** After an intent is whole, the script publishes the queue name on `<prefix>wake` for each due-now job it made ready (a plain `PUBLISH`, not part of the intent). `RedisStore` now implements `IWakeNotificationSource` for the work channel, so an idle worker usually starts within milliseconds of a due-now dispatch. Redis keeps no message for a listener that was not there, so polling stays the way correctness is guaranteed, as before.
7. **Idle sleep until the next due job.** The readiness probe now returns `NextDueAt`, the earliest job in the probed lanes that is not due yet, and an idle pool sleeps until then (never under the 50 ms probe spacing) instead of its idle back-off. PostgreSQL and SQLite leave it empty, so nothing changes for them.

Spec and public page: `docs/spec/22-redis.md`, `docs/spec/05-worker-engine.md`, `docs/Runwick.Docs/content/docs/redis.mdx`.

### Measured and left alone

- **Claim size and finish batching.** Both already batch up to 32. Batch size changes cost per job only a little (table above, 8 against 16), so a larger claim does not help; the fixed part per call is small now.
- **Claim plus begin in one call.** It already exists (`ReserveAndBegin`).
- **Round trips per job.** About 0.17 claim calls per drained job (3,455 for 20,000) and about 0.22 finish and probe calls (about 4,500), so the call count is not the problem.
- **Fsync.** On this host's disk one `appendfsync always` write takes about 7 to 10 ms, and many clients share one fsync (about 4,100 SETs/s with 50 clients, 143/s with one). Putting the log on tmpfs raised drain throughput only from 180 to 248 jobs/s, so fsync is a real cost but not the main one.
- **A hand-built reply for the claim result** (skipping the table that is then encoded) would save perhaps another 10% of script CPU but couples the script to the reply layout; not done.

### Drain of 20,000 jobs, 16 workers (three alternating rounds each, jobs/s)

| Log on | Runwick before | Runwick after | BullMQ 6.7.2 | After ÷ before | After ÷ BullMQ |
|---|---|---|---|---|---|
| host disk, fsync always | 136 (146, 134, 136) | 180 (180, 220, 171) | 697 (769, 546, 697) | 1.32× | 0.26× |
| tmpfs, fsync always (no disk wait) | 155 (155, 172, 152) | 248 (246, 289, 248) | 843 (809, 971, 843) | 1.60× | 0.29× |

Every "after" round beat its paired "before" round. The stored AOF size per run was the same before and after (about 354 MB, 17.7 KB per job) against 56 MB (2.8 KB per job) for BullMQ.

By an estimate from the run's command statistics, Redis was about 40% busy during the drain in the tmpfs "after" runs (about 25 s of script time in a 65 s drain), so the 16 workers are mostly waiting on claim and finish calls to come back rather than Redis being saturated. This is an inference, not a separate measurement: with 16 jobs in flight and a claim plus a finish taking tens of milliseconds each, the cycle time bounds throughput. BullMQ's scripts cost about 200 µs per call and its commands are tiny, so its calls return quickly and 16 workers keep Redis busy.

### Steady 500 offers/s and delayed 100 offers/s

Two and three alternating rounds, 16 callers and 16 workers. These are the noisiest numbers here; during the steady runs the host load average was above 20, and the "before" arm itself ran at 72 to 96 jobs/s, below the 160 jobs/s of the earlier report.

- **Steady 500.** Completions: before 96 and 73 (median 84), after 84 and 107 (median 96). Due-to-handler p99: before 35.8 s and 47.2 s, after 54.5 s and 30.4 s. BullMQ refused 5,900 to 7,700 of 15,000 locally and its offer-to-finish p99 was 1.0 to 1.2 s. No usable gain can be claimed; both Runwick arms are overloaded and far behind BullMQ.
- **Delayed 100, 5 s ahead.** Completions per second: before 81 (81, 85, 75), after 85 (85, 86, 61). Due-to-handler p99: before 4.95 s, 2.24 s, 5.94 s; after 1.94 s, 5.55 s, 7.37 s. The pickup lag spread is wider than any difference between arms, and round 3 of the "after" arm ran while the host was also running other suites (it fell behind its own offer rate and its enqueue p99 was 1.6 s). The mechanism (an idle pool now sleeps to the next due time) is tested and works, but this host could not show a stable gain in the lag, so none is claimed. BullMQ's offer-to-finish p99 was 0.6 to 0.7 s (this excludes the 5 s delay, which Runwick's "latencyMs" includes), so the comparison there is not like for like; the due-to-handler stage is the fair one.

### Tests

- New: `RedisWorkerPathTests` (2: the probe names the earliest not-yet-due job, and only a due-now dispatch publishes a wake) and one Core case (`A_store_that_names_the_next_due_job_ends_the_idle_wait_at_that_time`). The scripted store gained a `NextDueIn` switch.
- Results for the full Redis, Core and Contract projects are in the final message of this task and in `artifacts/perf/round-two/redis-worker-diet/tests-*.log`.

### What is left

- **Per-job script cost is still about five times BullMQ's.** What remains is the model, not the code: each command stores its after-images as a packed intent, then writes the same data again as it projects, and each job writes a receipt, an event and several index rows. That is why a job costs 17.7 KB of log against 2.8 KB. Cutting it needs the recovery model to change (for example intents only for commands that cannot finish inline, or an event stream that is not a hash plus a sorted set per event).
- **A claim that returns the next jobs together with the finish of the previous ones** (one call per settle) would halve the calls per job once scripts are cheaper. It changes `ReserveAndBegin` and `FinishMany` replies and the engine's slot logic, so it was not attempted here.
- **A quiet-host rerun** of steady 500 and delayed 100 is needed before any claim about the pickup lag.

### Evidence

Runner scripts, raw results and logs are in `artifacts/perf/round-two/redis-worker-diet/` in the worktree (git ignores artifacts); the profile, differential and paired-run scripts are tracked in `eng/bench-compare/redis-group/` (`profile_worker.py`, `diffcheck.py`, `pairs-worker.sh`, see its README).
