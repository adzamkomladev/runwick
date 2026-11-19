# Redis worker path: store less per job

Written 2026-11-19. Branch `worktree-agent-ad96d1e91ad0d86e1`, base `main` at 790c1ea.

## Short answer

The biggest thing each worker command wrote was not job data but a second copy of it: the stored "intent", a packed copy of every value the command was about to write plus its reply, written to the application's metadata hash before the real writes and cleared after them. It was about 55% of the claim's log bytes and 50% of a finish's. A command that finishes in one script call no longer stores it. Redis already writes a script's changes to the log and to replicas as one block, so nobody can see half of one call; the intent is now stored only when a write inside the script fails partway, which is the only case it still protects.

Effect: log bytes per drained job fall by more than half (claim 7.2 to 3.1 KB, FinishMany 4.4 to 2.1 KB), and script CPU per job by about 10 to 20%. In paired drains of 20,000 jobs, Redis script time per job fell about 16 to 20% (1.25 to 1.01 ms on a disk log; 0.84 ms on a memory log with buffered finish) and throughput rose by a median 1.05 to 1.23 times, with wide round-to-round spread. BullMQ is still about 2 times faster on a disk log and 4 times on a memory log, so the BullMQ target is not met.

No stored layout change, no migration, no reply or receipt change. Every test passes. Recommendation: merge as the default (it is already the default on this branch), with one open check on the delayed-pickup tail (see Benchmarks).

## What each command wrote, before and after

Measured with `eng/bench-compare/redis-group/writes.py`: real scripts, 20 calls of 8 jobs each against an AOF-always Redis 8.2, AOF bytes appended per call divided by jobs. "Read by" says who needs the piece.

| Piece (per job) | Claim before | Claim after | FinishMany before | FinishMany after | Read by | Decision |
|---|---:|---:|---:|---:|---|---|
| Stored intent (`meta` guard + packed intent + SHA) | 4,068 | 0 | 2,227 | 0 | recovery only, after a failed or multi-call projection | written only when needed (cut) |
| Publication (`meta` rev, clear guard/intent/sha/ipos) | 17 | 8 | 17 | 8 | readers (`rev`), history check | only `rev` now |
| Receipt row (`rcpt:<id>`) | 1,263 | 1,263 | 651 | 651 | replay after a lost reply | needed; the claim receipt is the claim reply |
| Job hash (attempt and job fields) | 889 | 889 | 875 | 735 | everything | needed; finish no longer deletes fields the job never had |
| Event rows (`events:data`) | 576 | 576 | 348 | 348 | `EventsSince`, dashboard timeline | needed (users see them) |
| Event index (`events`) | 42 | 42 | 24 | 24 | `EventsSince` | needed |
| State index (`idx:created:s:*`) | 113 | 113 | 114 | 114 | `ListJobs` by state, dashboard | needed |
| Lease index | 65 | 65 | 45 | 45 | expired-lease scan, recovery | needed |
| Ready index | 46 | 46 | – | – | claim | needed |
| Attempt → job (`attempts`) | 84 | 84 | – | – | management reads, RevealError | needed |
| Terminal index (`idx:terminal`) | – | – | 66 | 66 | job stats | needed |
| Receipt expiry index | 14 | 14 | 66 | 66 | retention | needed |
| MULTI/EXEC framing | 4 | 4 | 4 | 4 | Redis | – |
| **Total AOF bytes per job** | **7,190** | **3,113** | **4,443** | **2,067** | | |

Single `Finish`: 5,241 → 2,526 bytes. FinishAndClaim (8 finishes plus a claim of 8): 11,602 → 5,198 bytes per job.

Candidates considered and left alone:

- **The claim receipt (1.3 KB per job).** It holds the claim result that a resend must replay byte for byte. Shrinking it means changing the reply the C# client parses. Not done.
- **Attempt fields `token` and `closedby`** (about 150 bytes per job). No current reader, but they mirror PostgreSQL's attempt columns (spec 22 §3.1), so dropping them is a layout change for a 5% gain. Not done.
- **Event rows.** Two per claimed job (`attempt_reserved`, `attempt_started`) and one per finish; users see them. Not changed.
- **Index churn.** Already minimal in the log: Redis does not log a `ZREM` that removed nothing, and the in-script overlay already folds a `ZADD` followed by a `ZREM` of the same member.

## What was cut and why it is safe

1. **Direct projection.** A command whose writes fit its first call's budget (every worker command; most dispatches) writes them straight away and publishes with `HSET meta rev <n+1>`.
   - Redis puts all of one script call's writes into the log, and onto replicas, as one `MULTI`/`EXEC` block, and runs nothing else in between. A log whose last block is cut is refused at load (the profile requires `aof-load-truncated no`) and repaired by cutting back to the block before. So a crash, an AOF restart, a snapshot or a replica never holds part of one call.
   - What is left is a script failing between two of its own writes, which Redis does not undo. Out of memory cannot do it: a script with a shebang is checked only before it starts (checked on Redis 8.2: a script that crosses `maxmemory` halfway keeps writing; the next script is refused). `SCRIPT KILL` is refused once a script has written. Any other failing write (a key of the wrong type, a key that left a Cluster node) is caught: the script then stores the intent exactly as the old order would have stored it (guard, intent, `ipos` 1, the writes made so far already in place) and fails as before. From there recovery is unchanged.
   - Exactly-once, replay after a lost reply, fencing, lease renewal, cancellation, retries, events, retention: the decision and every value written are the same as before (`diffcheck.py`: 49 keys and 12 replies identical; the only differences are the four empty `meta` fields `guard`, `intent`, `intentsha`, `ipos` that are now never created).
   - The size limits (4 MiB intent, 512 KiB inline batch) still apply to the intent whether or not it is stored. An upper bound of the packed size, much cheaper than packing, settles them; when it cannot, the script packs as before.
   - Mixed versions: an intent stored by either binary has the same format and is finished by either. An older binary never reads the fields a direct projection leaves out.
   - The `FailAfter` test seam counts the guard that is no longer written as write 1, so a cut after N writes leaves the same state as before and every write-prefix test still means the same thing.
2. **No sorting of fields inside one write.** The order of fields in one `HSET` or members in one `ZADD` changes nothing stored. Sorting the job hash's fields cost about 25 to 60 µs per job.
3. **No deletes of fields a job never had.** Closing an attempt deleted `delay`, `next`, `error` and `reason` even when they were never written.

Migration: none. Layout revision, manifest and replies are unchanged.

## Tests

| Run | Result |
|---|---|
| Core | 453 of 453 |
| Full Redis project (own Redis 8.2 container, AOF always; server prepared by running one class first) | 124 of 124 (44 min) |
| Contract (SQLite, PostgreSQL via Testcontainers, Redis) | 496 of 496 |
| Dashboard (includes the Redis store snapshot) | 57 of 57 |
| `diffcheck.py` against main's scripts | identical data in 49 keys and 12 replies, apart from the four `meta` fields that are now never created |

New: `RedisDirectProjectionTests` (REDIS-19 in spec 22):

- DP-01: `ReserveAndBegin`, `FinishMany`, `Finish` and FinishAndClaim, each with its reply dropped after the script ran, leave no stored intent (`meta.intent` never written), and the resend replays the same answer.
- DP-02: a claim whose lease-index write hits a wrong key type stores its intent at that point. The application reads as Recovering. Once the key is repaired, the resend publishes the intent and replays the claim (3 jobs running, 3 leases).
- DP-03: the same for a finish batch whose terminal-index write fails. Each finish applies once.
- DP-04: every worker script and dispatch, run with a check that packs each intent, never finds the cheap size bound below the packed size.

The existing write-prefix sweeps (REDIS-01, FC-07, RAB-07), the wrong-key-type case (REDIS-04, now on a directly projected dispatch), memory full, crash and restart from the AOF, Sentinel and Cluster cases all pass unchanged.

## Benchmarks

Drain of 20,000 jobs, 1 KiB, 16 workers, fresh Redis 8.2 per run, `pairs-combined.sh`, two sets of three rounds per log kind. Arm order rotated each round, and every round held `/tmp/runwick-bench.lock`. Arms: `old` and `oldbuf` are main at 790c1ea without and with buffered finish. `new` and `newbuf` are this branch (`base` and `buf` in the result files). Load average at each run start was 4.5 to 15.7 on 8 cores (shared host, not quiet). Script time is Redis `commandstats` time for all scripts (insert included) divided by 20,000. AOF bytes include the insert.

| Log | Arm | Jobs/s, median of 6 | Script ms per job | Redis CPU ms per job | AOF bytes per job |
|---|---|---:|---:|---:|---:|
| disk | old | 409 | 1.25 | 1.41 to 1.52 | 17,740 |
| disk | new | 479 | 1.01 | 1.29 to 1.31 | 8,310 |
| disk | oldbuf | 469 | 1.19 | 1.38 to 1.50 | 17,460 |
| disk | newbuf | 558 | 0.94 | 1.13 to 1.26 | 8,165 |
| disk | BullMQ 6.7.2 (3 rounds) | 1,128 | 0.31 | 0.44 | 2,815 |
| tmpfs | old | 679 | 1.14 | 1.29 | 17,770 |
| tmpfs | new | 767 | 0.96 | 1.10 to 1.18 | 8,333 |
| tmpfs | oldbuf | 800 | 1.05 | 1.10 to 1.22 | 17,440 |
| tmpfs | newbuf | 902 | 0.84 | 0.90 to 1.02 | 8,158 |
| tmpfs | BullMQ (3 rounds) | 3,428 | 0.20 | 0.28 | 2,816 |

Paired ratios of jobs/s per round:

- Disk log, new ÷ old: 0.72, 1.13, 1.31, 1.26, 0.97, 0.93 (median 1.05).
- Disk log, newbuf ÷ oldbuf: 0.92, 0.98, 1.25, 1.49, 1.20, 1.30 (median 1.23).
- Memory log, new ÷ old: 1.15, 0.87, 1.14, 1.14, 1.13, 1.24 (median 1.14).
- Memory log, newbuf ÷ oldbuf: 1.48, 0.97, 1.08, 1.31, 0.92, 1.17 (median 1.12).

Script time and log bytes fell in every round. Throughput is noisier, because the 16 workers mostly wait on round trips rather than keeping Redis busy.

Script CPU inside the scripts (`profile_worker.py`, `os.clock`, 16 jobs per call): claim about 375 → 314 µs per job, FinishMany about 290 → 245, FinishAndClaim about 650 → 510. Of that, ordering the after-images went from about 1,000 to 140 to 220 µs per call, and the intent's guard write and packing (about 200 to 350 µs per call) are gone.

**Steady 500 offers/s (one round, old / new / BullMQ):** accepted 486 / 496 / 468 jobs/s; due-to-handler p99 16.4 s / 12.3 s; BullMQ offer-to-finish p99 0.23 s. Both Runwick arms are still overloaded at this rate.

**Delayed 100 offers/s, 5 s ahead (four rounds old/new, one with BullMQ):** throughput is equal (100 jobs/s; BullMQ 100). The due-to-handler tail is unstable in both arms. Old p99: 139, 102, 7,997 and 6,697 ms. New p99: 4,657, 5,143, 5,716 and 4,907 ms. BullMQ p99: 326 ms. New showed a tail of about 4 to 5 s in all four rounds; old showed one in two of four, and a worse one when it did. The code behind that wait (the pool's idle backoff and the probe's next-due hint) is client-side and unchanged here, and the earlier reports already saw 2 to 7 s tails on main. It may still interact with the cheaper scripts' timing (new made fewer claim and probe calls: 1,636 against 2,086 claims in round 1). Not resolved. It should be rechecked on a quiet host before anyone claims a delayed-pickup number.

## What is left

- Per job, a drained job still costs about 3 times BullMQ's script time (about 0.95 against 0.3 ms) and writes 3 times its log bytes (8.2 against 2.8 KB, insert included). What remains is spread out: decision logic about 35%, encoding the claim result for its receipt and reply about 20%, event rows about 10%, applying the writes about 15%.
- The next cuts all change something users or the protocol see: a smaller claim receipt (it is the claim reply, so the C# client would change), fewer or smaller event rows, and dropping the unread attempt fields `token` and `closedby` (a layout change for about 5%).
- The delayed-pickup tail (above) needs a quiet-host recheck. The engine's idle backoff when no job is known may be the real cause.
- Tools: `writes.py` (AOF bytes per job by key kind), `summarize_drain.py` now prints AOF bytes per job, and `profile_worker.py` understands the new commit path. Raw results are in `eng/bench-compare/redis-group/results/*-rsl*` (git-ignored).
