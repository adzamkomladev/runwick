## Redis: claim ahead of free slots (perf round two, Redis F)

Written 2026-11-23. Design: [redis-pool-claim-redesign.md](../../plans/redis-pool-claim-redesign.md).

### Short answer

There is a new opt-in setting, `WorkerOptions.ClaimAhead`, from 0 to 32, default 0. It lets a fixed pool claim that many jobs beyond its concurrency. The extra jobs wait in memory for a free handler slot. While they wait, their leases are renewed and fenced like those of running jobs. Handlers running at once never exceed the pool's concurrency.

The point is to carry more jobs in each Redis call and each fsync. On a disk log that is where Runwick loses most of its ground to BullMQ. Nothing stored, no script and no guarantee changes, and there is no migration.

**Not measured yet.** By the design note's model, the disk drain should move from about 0.55× to about 0.75–0.85× BullMQ, and the memory-log drain should barely move (about 0.38×). The paired benchmark below will show whether that holds.

### What it does

- **Claims.** The pool's ticket gate allows its target plus `ClaimAhead` tickets. The batched claim already asks for every free ticket, up to 32 jobs per call, so claims get bigger without any other change.
- **Waiting for a slot.** A claimed job waits for one of the pool's run slots before invocation admission (spec 05 §9.5). The run slot goes back when the handler returns, so the next waiting job starts without a round trip.
- **When a waiting job never runs.** It stops waiting and is not invoked once it has lost its lease, once a cancellation is observed, or once the drain cutoff has passed. In the last two cases it is finished as `UserCancelled` or `Interrupted`.
- **Visible differences** (why it is off by default):
  - A waiting job shows as running.
  - Its recorded start is the claim time.
  - It holds store-side concurrency and rate permits while it waits.
  - One busy worker can hold jobs that an idle worker could have run.
- **Automatic pools** ignore the setting.

### Tests

- **Core (fake store):**
  - `Claim_ahead_defaults_to_zero_and_is_validated` checks the default and rule V-26.
  - `Claim_ahead_holds_jobs_beyond_concurrency_but_runs_at_most_concurrency` uses 4 slots and claim ahead 4. It checks that 8 jobs are held but at most 4 handlers run at once, that every job runs once, and that every slot comes back.
  - `DefaultsMatchCodeTests` checks the 06 row "Prefetch beyond tickets".
- **Contract (real stores), `ClaimAheadContractTests`:**
  - **CA-01** (every store): claims beyond concurrency, never more handlers than concurrency, each job runs once, history invariants hold.
  - **CA-02** (SQLite and Redis): a claim's reply is lost after the store applied it. The resend replays it, each job runs once, one attempt per job.
  - **CA-03** (SQLite and Redis), the crash and fencing case:
    - A worker with 2 slots and claim ahead 4 holds 2 running and 4 waiting jobs.
    - It is then cut off from the store. Its claims, renewals and finishes never answer.
    - A second worker recovers all 6 jobs after their leases expire and runs them.
    - The first worker then reconnects. Its handlers return, but its 4 waiting jobs never start even though run slots are free. Every job ends succeeded with two attempts, and the history invariants hold.
- **Smoke run:** the three Core cases above passed. No suite or benchmark was run by the implementer.

### Benchmark

`eng/bench-compare/redis-group/pairs-combined.sh` has new arms:

- `caN`: buffered finish plus claim ahead N (for example `ca16`, `ca32`).
- `caNcif2`: the same with 2 claims in flight.

Paired against `buf` (main's best setting, same binary with the option off) and BullMQ:

```
cd eng/bench-compare/redis-group
dotnet build ../../../tests/Runwick.Workloads -c Release
FSYNC=disk  RESULT_TAG=-claimahead       ./pairs-combined.sh drain 5 ../../../tests/Runwick.Workloads/bin/Release/net10.0 buf ca16 ca32 ca16cif2 bullmq
FSYNC=tmpfs RESULT_TAG=-claimahead-mem   ./pairs-combined.sh drain 5 ../../../tests/Runwick.Workloads/bin/Release/net10.0 buf ca16 ca32 bullmq
./pairs-combined.sh steady500  3 ../../../tests/Runwick.Workloads/bin/Release/net10.0 buf ca16 bullmq
./pairs-combined.sh delayed100 3 ../../../tests/Runwick.Workloads/bin/Release/net10.0 buf ca16 bullmq
python3 summarize_drain.py results/drain-claimahead; python3 summarize_drain.py results/drain-claimahead-mem
python3 summarize_steady.py results/steady500; python3 summarize_steady.py results/delayed100
```

The bench JSON's `batchClaims` and `reservesSent` show the jobs per claim. With claim ahead it should rise from about 8 toward 16–32.
