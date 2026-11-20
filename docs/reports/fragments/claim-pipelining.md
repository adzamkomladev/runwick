# Claims in flight (claim pipelining)

## Short answer

- A worker pool can now keep more than one claim waiting for the store at once: `WorkerOptions.ClaimsInFlight`, 1 to 4. **The default stays 1**, which is exactly the old behaviour.
- With **2 in flight**, a Redis drain at 16 slots with buffered finish ran **1.18× faster on a disk log and 1.30× faster with the log in memory** (median of paired rounds against main). PostgreSQL drained **1.08× faster** (1.13× with 4 in flight).
- **SQLite did not gain** (0.98× at 2, 0.73× at 4). Its single writer makes overlapping claims wait for each other.
- On Redis, **delayed jobs were picked up a little later** with 2 in flight: p95 higher in all 4 rounds, p99 median 399 ms against 310 ms. Throughput was the same.
- Ratio to BullMQ on Redis (16 slots and concurrency 16, buffered finish): disk **0.30 → 0.52**, memory **0.30 → 0.45**. The host was heavily loaded during these runs (load 9 to 72), so the absolute numbers are low for every arm. The paired ratios are the useful figures.
- So the option helps where a pool waits on claim round trips (Redis, PostgreSQL), costs pickup delay, and hurts SQLite. It is not on by default.

## What changed

Before, a pool sent one claim (`ReserveAndBegin`, which claims and starts up to 32 jobs in one call) and waited for its reply before it looked for more work. Slots that freed up during that wait stayed empty until the reply came back.

With `ClaimsInFlight = 2` or more:

- The pool sends a claim and goes on. Slots that free up while it waits can be claimed by another request at once. Replies are applied one at a time, under the same lock as before.
- **A request sent next to another must be large.** It is sent only when at least `target ÷ ClaimsInFlight` slots are free (8 of 16 at 2). The first version had no such floor. It cut the jobs per claim from 8.4 to 5.3 (2 in flight) and 3.1 (4 in flight), raised Redis script time per job by about 20%, and was no faster on Redis (disk: 378 jobs/s main, 358 at 2, 349 at 4). With the floor, claims stay near main's size (about 2,700 claims per 20,000 jobs against 2,420).
- Only the simple case overlaps: one queue and one priority have work. When several lanes have work, the pool claims one job at a time and waits, as before.

The spec is 05 §9.7 (Changes 2026-11-20); the default and the new rule V-25 are in 06.

## Why it is safe

| Guarantee | How it is kept |
|---|---|
| Never claim more than free slots | Each request takes slot tickets before it is sent and asks for exactly that many jobs. There is no claim-ahead: prefetch stays 0. A test checks that the jobs asked for by the requests in flight plus the slots held never exceed the pool size, at 1, 2 and 4 in flight. |
| Lane fairness and aging | Overlap happens only when one lane is eligible; any choice between lanes waits for its own reply. Share counters are updated when a reply is applied. The CAP-02 fairness case (a hidden queue returns with exactly 1 of 4 admissions) passes at 1 and 2 in flight. |
| Per-queue concurrency and rate limits | Enforced by the store per claim, unchanged. Each claim is its own command with its own receipt. |
| Pool target and slot accounting | Unchanged tickets; every reply path returns the tickets it did not use. Tests end with zero outstanding tickets and zero claims in flight. |
| Cancellation and shutdown | Drain stops new requests. Shutdown waits for every request in flight and for its reply to be applied. A request still waiting for an I/O slot when admission closes is not sent. Tested with a claim held at the store until the drain begins. |
| Lease timing of claimed jobs | Every claimed job becomes a tracked attempt as soon as its reply is applied, and its lease deadline counts from when the request was sent. A reply that waits for the lock shortens the deadline instead of stretching it. No claimed job waits for a slot. |
| Lost or slow replies | Each request resolves its own command by resending it with the same ID, as before. The pool stops claiming while any of its requests is unresolved past the resolution budget. Tested with a lost reply and with a request that hangs until its timeout while another is in flight; every job ran exactly once. |
| Idle back-off and wake hints | A request sent without waiting does not reset the idle back-off; an applied reply does, as before. A "no work" reply sets the lane aside only under the readiness snapshot it was chosen from. (Without that rule, a late reply could hide a lane that a newer snapshot shows has work. One delayed100 round showed a 2.5 s pickup p99 before this fix; none after.) |

## Tests

On the branch rebased onto origin/main (1d45bc0):

| Suite | Result |
|---|---|
| Core | 464 passed, 0 failed |
| Contract | 502 passed, 0 failed |
| SQLite | 56 passed, 0 failed |
| Redis (Redis 8.2, `appendfsync always`) | 131 passed, 0 failed |
| PostgreSQL | 406 passed, 0 failed |

By mistake, two full runs overlapped and shared one Redis container. In that overlap, one Redis test and the SQLite writer-timing test (`While_another_process_holds_the_writer_…`) each failed once. Each passed when its suite ran again by itself. The default is 1, so these stores take the old code path.

New Core tests (`WorkerEngineTests`, `CapacityCaseTests`): option default and V-25; requests only fill free slots (1, 2 and 4 in flight); with 2 in flight a drain finishes sooner in fake time than with 1; shutdown with a claim outstanding; a claim whose reply is lost, or that times out, while another is in flight; CAP-02 at 2 in flight.

## Benchmarks

Paired runs, arm order rotated each round, every round under `flock /tmp/runwick-bench.lock`, load average recorded at the start of each run, kernel disk pressure (PSI) before and after each Redis run. Base: origin/main at 1d45bc0. Candidate: this branch. 20,000 jobs, 1 KiB, 16 slots, one producer call. Redis 8.2 with `appendfsync always`, fresh per run. Tools: `eng/bench-compare/redis-group/pairs-combined.sh` (arms `oldbuf`, `cif2`, `bullmq`) and `eng/bench-compare/claims-pairs.sh` (PostgreSQL 18 in its own container, SQLite on a fresh file).

**Redis drain, disk log, buffered finish, 5 rounds**

| Arm | Jobs/s per round | Median | Script ms/job | Claims | Load at start |
|---|---|---:|---:|---:|---|
| main | 162, 387, 274, 427, 394 | 387 | 1.18 | 2,424 | 37, 19, 15, 21, 46 |
| 2 in flight | 327, 427, 426, 504, 253 | 426 | 1.09 | 2,673 | 42, 27, 26, 14, 17 |
| BullMQ 6.7.2 | 629, 759, 1,161, 971, 1,334 | 971 | 0.36 | – | 42, 22, 19, 20, 66 |

| Pair | Per round | Median |
|---|---|---:|
| 2 in flight / main | 2.01, 1.10, 1.56, 1.18, 0.64 | **1.18** |
| 2 in flight / BullMQ | 0.52, 0.56, 0.37, 0.52, 0.19 | **0.52** |
| main / BullMQ | 0.26, 0.51, 0.24, 0.44, 0.30 | **0.30** |

**Redis drain, log in memory (FSYNC=tmpfs), buffered finish, 4 rounds**

| Arm | Jobs/s per round | Median | Script ms/job | Claims | Load at start |
|---|---|---:|---:|---:|---|
| main | 980, 640, 824, 879 | 851 | 0.83 | 2,396 | 72, 24, 15, 11 |
| 2 in flight | 1,153, 1,249, 1,083, 1,129 | 1,141 | 0.74 | 2,668 | 62, 37, 12, 9 |
| BullMQ 6.7.2 | 2,306, 3,170, 2,145, 4,156 | 2,738 | 0.23 | – | 43, 28, 17, 9 |

| Pair | Per round | Median |
|---|---|---:|
| 2 in flight / main | 1.18, 1.95, 1.32, 1.28 | **1.30** |
| 2 in flight / BullMQ | 0.50, 0.39, 0.51, 0.27 | **0.45** |
| main / BullMQ | 0.43, 0.20, 0.38, 0.21 | **0.30** |

Main's ratio to BullMQ here (0.30) is lower than the 0.41 in the stored-data design. The host was much busier during these runs, and BullMQ ran very fast in some rounds.

**Redis delayed100 (100 jobs/s, each due 5 s after dispatch), after the "no work" fix, 4 rounds.** Pickup is the time from when a job is due to when its handler starts.

| Round | main p95 / p99 ms | 2 in flight p95 / p99 ms | Jobs/s (both) | Load at start |
|---|---|---|---|---|
| 1 | 163 / 309 | 245 / 442 | 85.7 / 85.6 | 63 / 51 |
| 2 | 203 / 312 | 268 / 356 | 85.6 / 85.6 | 51 / 53 |
| 3 | 224 / 573 | 244 / 472 | 85.5 / 85.6 | 45 / 35 |
| 4 | 135 / 261 | 157 / 307 | 85.6 / 85.6 | 42 / 36 |

The p95 is higher with 2 in flight in every round; the p99 medians are 310 and 399 ms. Before the fix, 3 rounds gave p99 of 133, 482 and 244 ms (main) against 439, 358 and 2,471 ms; the 2.5 s round also lost throughput (80 jobs/s). BullMQ's own delayed figures (p99 342 to 926 ms) are measured differently and are not comparable.

**PostgreSQL drain, default finish, 3 rounds**

| Arm | Jobs/s per round | Median | Ratio to main per round | Median ratio |
|---|---|---:|---|---:|
| main | 219.8, 260.4, 206.3 | 219.8 | – | – |
| 2 in flight | 238.4, 271.7, 263.1 | 263.1 | 1.08, 1.04, 1.28 | **1.08** |
| 4 in flight | 231.7, 295.0, 291.8 | 291.8 | 1.05, 1.13, 1.41 | **1.13** |

**SQLite drain, default finish, 3 rounds**

| Arm | Jobs/s per round | Median | Ratio to main per round | Median ratio |
|---|---|---:|---|---:|
| main | 231.4, 225.2, 260.7 | 231.4 | – | – |
| 2 in flight | 239.4, 220.0, 205.6 | 220.0 | 1.03, 0.98, 0.79 | **0.98** |
| 4 in flight | 168.3, 191.0, 173.9 | 173.9 | 0.73, 0.85, 0.67 | **0.73** |

Load at the start of the PostgreSQL and SQLite runs was 5 to 13.

**Earlier rounds on the previous base (ff1374d, before origin/main's Redis script cuts).** Redis without buffered finish, 3 rounds: 2 in flight against main 1.06, 1.42, 0.82, so no clear effect. Without buffered finish, slots free up in bursts when a batch of finishes is confirmed, and claims are already large. Two disk rounds there were 3 to 7 times slower for 2 in flight (205 s and 374 s against about 69 s for main). Redis was mostly idle during those runs, the producer's health check also timed out, and host load was 14 to 129. Six further disk rounds with disk pressure recorded (PSI 3 to 5%) had 2 in flight faster in every round (1.03 to 2.69). That points to disk contention on the host, not the engine, but it was not proven.

## Decision on the default

The default stays **1**. Two in flight is a clear win only for Redis with buffered finish and a modest one for PostgreSQL. It slows SQLite, and it delays the pickup of due jobs a little. A store-dependent default would need its own option design; that is left for the user to decide.

## What's left

- **A per-store default**, for example 2 for Redis with buffered finish, needs a decision and probably an option that can mean "the store's choice".
- **The delayed pickup tail.** Why p95 and p99 rise with 2 in flight at a low arrival rate (each due job is claimed alone, so overlap only adds requests) is not explained. Worth one traced run.
- **The two very slow disk rounds** on the earlier base were not reproduced and not explained.
- **Quieter host.** Every set ran at load 9 to 72. More rounds on a quieter host would narrow the ratios.
- Not measured: steady500, unbuffered Redis on the new base, 32 slots with claims in flight, and the combined finish-and-claim option (which declines while a pipelined claim is out).
