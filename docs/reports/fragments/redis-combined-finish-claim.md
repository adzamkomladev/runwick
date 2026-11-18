## Redis combined finish and claim (work area F follow-up)

Written 2026-11-18. Informational: measured on the shared host while other sessions ran test suites and benchmarks (load average 20 to 75 on 8 cores). Each round ran all its arms one after another under the shared benchmark lock, with the order rotating between rounds. The numbers show direction and orders of magnitude, not exact gains.

### Short answer

The option is built, tested and safe, but it does not close the gap to BullMQ. Across nine paired drain rounds the combined call changed throughput by between 0.76 and 1.33 times the baseline, with a median close to 1.0. BullMQ stayed 3 times (disk) to 6 times (memory log) faster than Runwick in the same rounds. The reason is that the pool's own claim loop is busy claiming most of the time, so a finishing worker can attach its claim in only about one case in four (see "Why it helps so little"). It stays an opt-in and is not a recommended setting yet.

### What it does

With `WorkerOptions.UseCombinedFinishClaim()` (off by default), a worker that is about to send the finishes of attempts that just ended can add to the same Redis call the claim of that many new jobs. The finished attempts' slots are the claim's slots, so the next job starts without waiting for a separate round trip. Only Redis supports it; with any other store the option does nothing and the worker finishes and claims separately as before.

- **One script call, one intent.** The new script `finish_and_claim.lua` runs the finish items of `FinishMany` and then the claim of `ReserveAndBegin`, inside one script call and one stored intent, so both halves apply or neither does. A permit that a finish frees is visible to the claim in the same call. The shared decision code moved into `jobs.lua`, and `finish_many.lua` and `reserve_and_begin.lua` now call it (their behaviour is unchanged; the existing tests pass).
- **Every command keeps its own receipt.** Each finish has its own durable `Finish` receipt. The claim is framed as the ordinary `ReserveAndBegin` command, so its receipt is an ordinary claim receipt holding only the claim result; sending the same claim command alone later replays it. When no job is claimed (no work, paused queue, capacity blocked) the finishes still apply, the reply carries the claim's status, and no claim receipt is written, as for any claim that did nothing.
- **No stored layout change.** No migration, no new key, no new field. Older workers and the dashboard read the same data.
- **Engine side.** The completion worker takes the finishes of one pool that already wait in the ready channel. It asks the pool for a claim of that many jobs (`TryBeginInlineClaim`). The pool agrees only when the loop itself would batch the same way right now: admission open, not draining, not suspended, the readiness hint is fresh (no probe is started), exactly one lane is claimable and it is not an aging pick, and no claim of that pool is in flight (the call takes the pool's claim gate without waiting). Otherwise the finishes go the ordinary way. An Applied claim moves each used slot to the new attempt before any finish in the call completes, so the finisher's later release of that slot does nothing.
- **Failure.** A refused call (`UnsupportedVersion`, `InvalidRequest`) turns the option off for that worker and every finish resolves alone. A failed or lost call resolves each finish by its own command and the claim by sending the same claim command again, which replays if the call committed and applies if it did not. A replayed claim carries no finish results; each finish then resolves from its own receipt.

Spec: 03 (Changes 2026-11-17), 05 §12.7, 22 (Changes). Public docs: configuration page.

### Tests

| Run | Result |
|---|---|
| `RedisFinishAndClaimTests` (store level, real Redis, 7) | 7 of 7 |
| `FinishAndClaimContractTests` (worker level: SQLite, PostgreSQL, Redis, a refused call, a lost reply; 5) | 5 of 5 |
| Full Redis project | 105 of 105 (2 h 57 m under load) |
| Core | 453 of 453 |
| Contract, Redis kind (`RUNWICK_TEST_STORES=Redis`) | 168 passed; the 4 reported "No data found" are tests that use only SQLite or PostgreSQL, which the filter empties |

The store-level cases: finishes and claim decided together with their own receipts; a resent call replays the claim alone and each finish from its own receipt; a plain claim with the claim command's ID replays it; no work still finishes and leaves no claim receipt; a stale lease is refused alone while the claim applies; the claim sees the permit a finish in the same call frees (a resource with one permit is blocked before, claimed after); invalid calls change nothing; and every write prefix of the call from 1 to 40 (the Redis form of "crash between the halves") leaves each job finished once and each new job started once after the resend. The worker-level cases check that 80 jobs on every store settle exactly once with one finish receipt each (and that Redis really used the call), that a refused call falls back and finishes everything, and that a lost reply resolves without running anything twice. A rate-limited claim item goes through the same script function as a plain claim; no separate case was written for it.

### Measured (drain of 20,000 jobs, 1 KiB, 16 workers, one call per producer, fresh Redis 8.2 per run)

Median jobs per second; the ratio is the combined arm over the baseline in each round.

| Set | Baseline | Combined | BullMQ 6.7.2 | Combined ÷ baseline, per round | Combined ÷ BullMQ |
|---|---|---|---|---|---|
| Disk log, rounds 1 to 3 (load 3 to 59) | 110 | 99 | 332 | 1.11, 0.90, 0.76 | 0.30 |
| Memory log (fsync almost free), rounds 1 to 3 (load 8 to 51) | 189 | 212 | 1,251 | 1.33, 0.97, 1.02 | 0.17 |
| First disk set, run while my own test suite ran (load 30 to 75) | 28 | 58 | 157 | 2.42, 0.90, 1.95 | 0.37 |

A short extra set also ran buffered finish (`UseBufferedFinish`, G1) beside the combined option on a disk log, two rounds each: baseline 54, combined 63, buffered 68, both 73 jobs per second. Two rounds on a busy host cannot separate these.

The first set is shown for completeness; it ran while the host was heavily loaded by my own tests and swings by more than 2 times between rounds, so the other two sets are the ones to read. Raw results are in `eng/bench-compare/redis-group/results/` (drain, drain-r2, drain-tmpfs, drain-g1, drain-diag, drain-diag2) in the worktree and are not committed.

### Why it helps so little

A counter run (memory log, one round each) shows the combined call being used and also how rarely:

- Baseline: 3,161 claim calls plus 1,537 FinishMany calls for 20,000 jobs.
- Combined arm: 720 combined calls, 2,295 plain claim calls and 1,507 FinishMany calls. About a quarter of the claims (720 of 3,015) rode on a finish, but the combined calls themselves are calls, so the total fell only from about 4,700 to about 4,520 (single Finish calls not counted in either).
- The pool declined the combined attempt 3,930 times because its claim gate was taken (the loop was claiming), 17 times because the readiness hint was stale, and never because of the lane shape or a closed admission.

So the loop that fills free slots is busy claiming most of the time, and a finisher that finds it busy sends its finish alone as before. Where the call is used, it saves a claim round trip, but the claims that remain are still one call per batch, and a drained job still costs about five times BullMQ's script time (see the worker path section). The fsync wait is paid per call, and the call count fell by only a few percent.

### What is left

- **Let the pool carry the finishes instead of the finishers carrying the claim.** With buffered finish a finished attempt gives its slot back at once, so the loop has free slots while the finishes are still waiting in the channel. If the loop's claim took those finishes with it, nearly every claim would carry them. That is a larger change to the claim loop and to buffered finish and was not attempted here. The "both" arm above hints that the two modes may add up; two noisy rounds do not prove it.
- **Waiting for the claim gate** (a short bounded wait instead of declining) was considered and not built: it would make the finish wait behind the loop's claim, which lengthens the time a slot is occupied and is likely to lose what the saved call gains.
- **The cost per drained job** (about 1.1 ms of script time and about 17 KB of log against BullMQ's 0.2 ms and 2.8 KB) is untouched by this work and remains the main gap.
- A quiet-host repeat, a measurement with the option on steady and delayed workloads, and a power-loss comparison were not done.

### Tools

`eng/bench-compare/redis-group/pairs-combined.sh <workload> <rounds> <bench-bin-dir> [arms]` runs one Runwick bench build with and without the options (arms `base`, `comb`, `buf`, `both`) and BullMQ, alternating the order, with a fresh Redis 8.2 per run and the shared benchmark lock held for each round. `--combined-finish-claim` is the bench flag. The bench output now reports `finishesSent`, `finishManyCalls`, `combinedCalls`, `combinedClaimed`, `combinedFallbacks` and `inlineDeclined` (why the pool declined).
