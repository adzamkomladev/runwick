## Buffered finish (G1)

**Status 2026-11-15:** landed as an opt-in worker profile, `WorkerOptions.UseBufferedFinish()`. Off by default. The default behaviour and every default value are unchanged.

### What it does

Today a job holds its worker slot until the database has confirmed that the job finished. With buffered finish, the slot goes back to the pool the moment the handler returns and the finish is queued. The finishes are then sent to the store together, about every 25 ms, or sooner when 32 have collected. The next job starts while the previous finish is still on its way.

The job itself is not treated as finished until the store says so. While its finish is waiting, the job keeps everything that fences it: its entry in the worker, its lease, its concurrency and rate permits, and its unique key. Only the local slot is reused. This is what the plan asked for ("remote permits remain held until storage accepts the finish").

### Limits, so memory and risk stay bounded

- At most 256 unsettled buffered finishes (setting: 1 to 4,096).
- At most 4 MiB of estimated finish size, counting error text (setting: 64 KiB to 64 MiB).
- If the oldest buffered finish is more than 1 s old, no more slots are given back until it settles.
- When any limit is hit the slot is simply kept until the finish settles, which is exactly what the default does. That is how refilling stops. Nothing is dropped or queued without bound.
- A partial batch never waits past its flush interval (25 ms by default, 1 ms to 1 s), and it is sent at once if the worker is shutting down or any collected job is within about 13 seconds of its local authority deadline.

### What it can lose

A worker that dies while finishes are still only in its memory has not told the store those jobs finished. Their leases run out, recovery returns them to retry, and the handler runs again. This is the same at-least-once behaviour Runwick always has, but the window is wider: up to the buffer, a few tens of milliseconds of completed work. Handlers must be safe to run twice. A normal shutdown sends the buffer first. If the store does not answer during shutdown, the outcome is unresolved, just as without the option.

### Tests

- Core (fake time, scripted store with `FinishMany`): off by default and its limits are validated (V-22); a held store lets later jobs start only with the option on (1 job against 6); the count limit stops slot reuse at the bound and everything settles once the store accepts (4 of 10 started while held, 10 of 10 settled after); the byte limit holds; simultaneous finishes go out in one call and a lone finish waits only the interval; a drain flushes at once; a buffered attempt stays tracked in `Finishing`, fenced and unretired until accepted; finishes that never reach the store leave the job to expire and be recovered with no finish recorded. 8 of 8 pass; the Core project passes 449 of 449.
- Contract, SQLite, PostgreSQL and Redis: 60 jobs settle once each with one Finish receipt each and the buffer empties; a full buffer (limit 2, 8 slots) keeps slots and loses nothing; a graceful stop sends 16 buffered finishes; on SQLite and Redis, a worker whose finishes never reached the store leaves 8 jobs that another worker recovers and runs again (each job then has two attempts). 11 of 11 pass. The neighbouring finish-batching and worker contract tests (45) still pass.
- Not covered on a real store: a hard kill of the worker process (the crash case stops the first worker normally while its finishes are held; PostgreSQL has no equivalent hold in the harness).

### Measurement

Paired default against buffered drain runs, alternating order, 4,000 jobs of 512 bytes, 4 and 16 workers, on SQLite and PostgreSQL 18 (docker, same host). Every run took `flock /tmp/runwick-bench.lock`. The host is shared with the running SP-029 qualification and other agents' work, so the numbers are informational. Raw files: `artifacts/perf/round-two/g1-buffered-finish/` in the G1 worktree.

**Results are not in yet.** The 40 runs (2 stores × 2 worker counts × 5 pairs × 2 arms) are queued behind other agents' benchmarks on the shared lock. `artifacts/perf/round-two/g1-buffered-finish/pairs.sh` runs them and writes `run.done` when finished. `python3 summarize.py` in that folder then writes `summary.json` with the medians and the per-pair ratios. Replace this paragraph with that table, and say plainly if the ratios straddle 1.0.

### Open

- Redis was not benchmarked for G1; its contract tests pass.
- A hard-kill test of a real worker process is not done.
- Interaction with auto-concurrency: the scaler sees lower occupancy during the finish round trip. Not measured.
- Whether to make this a per-queue setting instead of per worker. Today it applies to every pool of the worker.
