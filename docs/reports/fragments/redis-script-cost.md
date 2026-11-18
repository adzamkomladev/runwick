## Redis: where the worker scripts spend their time, and what was cut (work area F follow-up)

Written 2026-11-18. Measured on the shared host. Unlike the earlier Redis reports, most of this ran while the host was nearly idle (load average 2 to 16 on 8 cores, recorded per run), which turned out to matter a lot. Every timed drain held the shared bench lock.

### Short answer

- **The "4 to 5 ms of script time per job" was mostly the busy host, not the scripts.** On a quiet host the same drain (20,000 jobs, 1 KiB, 16 workers, main as of 9ec2a34) costs about 1.3 to 1.7 ms of Redis script time per job, and that includes inserting the 20,000 jobs (about 0.2 ms per job). Redis's `INFO commandstats` time is wall-clock time inside Redis. When the host is overloaded, Redis's own thread is paused in the middle of scripts and runs slower when it does run, so the same work reads as about three times more. Redis's own CPU counter (`used_cpu_user_main_thread`) also went from about 29 s per drain on a quiet host to about 80 s on the loaded one.
- **The worker path (claim plus finish) costs about 1.1 ms per job on main. Almost all of it is Lua code, not Redis commands.** The Redis commands a script issues (about 16 per drained job) account for roughly 11% of script time. The rest goes on building and sorting JSON, sorting hash fields, keeping the in-script overlay, packing the stored intent, and running the decision itself.
- **Three changes cut worker-script CPU by about 20 to 27% per job, with byte-identical stored data and reply text.** In paired drains on a disk log, throughput rose 1.46 times without buffered finish (303 to 442 jobs/s, median) and 1.39 times with it (328 to 456). Script time per job fell from 1.68 to 1.17 ms and from 1.71 to 1.25 ms. BullMQ in the same rounds: 1,438 jobs/s and 0.29 ms per job. With the log kept in memory, the gain is smaller and noisier: 1.17 times without buffered finish and 1.31 times with it, over six rounds.
- **Why `finish_and_claim` looked dearer than FinishMany plus a claim:** (1) it encoded the claimed jobs' result twice, once for the claim receipt and once more for the reply that adds the finished list. That was about 1.1 ms per call at 8 jobs, about 12% of the call. (2) The 5.2 against 4.1 ms came from loaded-host rounds. On a quiet host the same two arms of that branch cost 1.49 and 1.40 ms per job (6% apart). Per job, the combined call cost the same as the two separate calls (1,187 against 1,178 µs): the second encode ate the saving from making one call instead of two. With the second encode removed, a combined call now costs 0 to 10% less per job than the two calls.

### What was measured

Tools, all in `eng/bench-compare/redis-group/`:

- `slowdrain.sh`: one real drain with Redis's slow log set to record every command, then Redis time per operation (`slowparse.py`).
- `mondrain.sh`: a 2,000-job drain under `MONITOR`, counting the Redis commands each script issues (`monparse.py`).
- `profile_worker.py`: CPU time inside the real scripts, by phase. It is CPU time (`os.clock`), so host load distorts it much less. New flags: `--combined` also times `finish_and_claim`, and `WRAPS=enc,sorted,...` also times chosen helpers.
- `perf record` on a Redis 8.2 build with symbols, to see which C functions the Lua work lands in.

**Per script on main, quiet host, buffered finish on (one drain, Redis time from the slow log):**

| Script | Calls per job | µs per call | µs per job | Redis commands per call (per job) | Biggest costs inside |
|---|---|---|---|---|---|
| ReserveAndBegin (claim, about 8.3 jobs per call) | 0.12 | 5,254 | 635 | 61 (7.2) | encoding the claim result for the receipt (about 130 µs per job), sorting each job hash's fields before writing (about 100), reserve and begin decisions (about 125), projection writes (about 40) |
| FinishMany (about 18.5 finishes per call) | 0.05 | 9,061 | 487 | 88 (5.8) | decision (about 150 per job: reading the job hash, closing the attempt, encoding each result, the receipt row), sorting job hash and receipt fields (about 105), packing the intent (about 35), projection (about 45) |
| DispatchMany (insert, 100 per call) | 0.01 | 21,618 | 217 | 397 (4.2) | not looked at here (the dispatch diet covered it) |
| Finish (single, at the drain's end) | 0.004 | 1,190 | 5 | 17 | |
| Total script time | | | 1,350 | | |

Without buffered finish the claim is the same, and there are about 2.5 times as many FinishMany calls, each smaller. Script time per job is about the same (27.2 s against 26.9 s per drain).

**Where the CPU goes (perf, whole drain):** the Lua interpreter loop 14%, creating strings 7% (every `..`, `string.sub` and `string.format` makes a new string), function calls 6%, table lookups 4%, `string.byte` 3% (the byte comparator), pattern matching about 2.5% (the JSON string escaper's check), memory allocation about 6%, garbage collection about 2%, SHA-1 of the intent 1%. Nothing is O(backlog): the ready-set scans read one page of 100, and each sorted-set write is O(log n).

**Phase breakdown from `profile_worker.py` (CPU µs per job, 16 jobs per call, before the change):**

| Phase | Claim | Finish |
|---|---|---|
| Frame before the decision (meta read, gates) | 8 | 6 |
| Decision | 125 | 150 |
| Encoding the result for the receipt and reply | 130 | (inside the decision) |
| Event rows | 22 | 17 |
| Ordering the after-images (mostly the field sort) | 102 | 105 |
| Packing the intent | 13 | 34 |
| Writing the guard and projecting | 52 | 53 |
| Total | 453 | 368 |

### What was changed (commit 5bf91db, scripts only)

All of it is in `src/Runwick.Redis/Lua`. Nothing stored changes, no reply changes, and the order of the after-images in the intent does not change. `diffcheck.py` (now also comparing the exact reply text and covering `finish_and_claim` and its replay) shows 0 differences in 49 keys and 12 replies against main's scripts.

1. **The JSON writer fills one buffer.** `U.enc` used to build a string for every value and every object, then join them again one level up. It now appends pieces to one table and joins once. Each distinct key's `"key":` text is built once per call.
2. **Each key set is sorted once per call.** Object keys must come out in byte order. The writer used to sort every object's keys with the Lua comparator, and a claim of 16 jobs has 16 copies of each object shape. The writer now walks a small per-call tree by the object's keys in the order Lua lists them, and keeps the sorted list at the end of that path. The result is exact for any key set. If Lua lists the same keys in another order, that only costs one more sort. The tree lives in script locals, so nothing carries over between calls.
3. **The byte comparator compares eight bytes per call.** After the first byte, `U.blt` now reads eight bytes of each string per `string.byte` call instead of one. Job hash fields share a 35-character prefix (`a:<attempt id>:`), so this is where field sorting spent its time. Sorting a job hash's 18 fields went from 73 to 27 µs. It still compares byte values, never strings with Lua `<` (REDIS-11).
4. **FinishMany receipt rows** are written from the receipt's fixed, already ordered field list (`U.hset_sorted`, as single finishes and dispatch already did), so they are not sorted again.
5. **`finish_and_claim` builds its reply text by adding `"finished":[...]` to the claim receipt's text** (the key orders before `items`, and it checks that `items` is the only other key) instead of encoding the claimed jobs again.

**CPU per job after the change (`profile_worker.py`, base and new scripts back to back, two rounds, medians):**

| Jobs per call | Claim before → after | Finish before → after | Combined call (one finish plus one claim per job) before → after | Claim plus finish as two calls, after |
|---|---|---|---|---|
| 8 | 538 → 392 µs (−27%) | 407 → 342 (−16%) | 1,060 → 657 (−38%) | 734 |
| 16 | 497 → 371 (−25%) | 378 → 299 (−21%) | 1,002 → 675 (−33%) | 670 |

Before the change the combined call cost 12 to 22% more per job than the two separate calls. Now it costs between 10% less and about the same.

### Measured and left alone

- **Deletions of fields that do not exist.** A finish deletes up to four attempt fields that were never written (`delay`, `error`, `next` and similar for a success). Skipping them would save a few µs per job. It would also change the intent's contents against main, so it was left.
- **The remaining sort of each job hash's fields** (about 27 µs per job hash per call). Removing it needs a fixed, pre-ordered write list across reserve, begin and close, which means restructuring how the job hash is written.
- **The JSON escaper's pattern check** (about 0.6 µs per string, about 15 µs per claimed job). A per-call cache only helps for repeated values.
- **The fixed part of every call** (about 100 to 150 µs: building the chunk's functions, reading meta, the gates). Spread over 8 to 16 jobs, it is under 15 µs per job.
- **The model itself**: the packed intent, then the same data written again as it is projected; a receipt, an event row and an event index entry per command; four index sorted sets per state change. This is still most of the gap to BullMQ, as the earlier reports said.

### Tests

| Run | Result |
|---|---|
| Core | 453 of 453 |
| `FinishAndClaimContractTests` (SQLite, PostgreSQL, Redis, a refused call, a lost reply) | 5 of 5 |
| Full Redis project (own Redis 8.2 container, AOF always) | 120 of 120 (35 m 46 s) |
| `diffcheck.py` against main's scripts | 0 differences (49 keys, 12 replies) |

A first full Redis run against a brand-new, empty server failed 52 of 120 tests, all with "The Redis database has rw:* keys but no rw:store". This is a test-setup race, not the change. On an empty server, test classes start in parallel and some write `rw:*` keys before the shared fixture has migrated the server. After that, every later migration refuses. The usual test server (127.0.0.1:56379) is long-lived and already migrated, so it never shows this. I migrated the new server first by running one class alone (`RedisDispatchLayoutTests`, 2 of 2), and then the whole project passed. Worth a fixture fix (migrate once before any class starts); it was not changed here.

### Benchmark: drain of 20,000 jobs, 1 KiB, 16 workers, fresh Redis 8.2 per run

Arms: `old` and `oldbuf` are main at 9ec2a34 without and with buffered finish; `new` and `buf` are this change without and with buffered finish. BullMQ is 6.7.2. Each round runs all five arms back to back under the bench lock, in rotated order. "Script time" is Redis's `commandstats` time for all scripts in the run (insert included) divided by 20,000. "CPU" is Redis main-thread CPU per job.

**Disk log (appendfsync always on the host disk)**

| Arm | Jobs/s per round | Median | Script ms per job | CPU ms per job | Load at start |
|---|---|---|---|---|---|
| old | 303, 301, 388 | 303 | 1.68 | 1.81 | 11.7, 8.3, 10.6 |
| new | 356, 442, 458 | 442 | 1.17 | 1.44 | 13.7, 10.5, 12.3 |
| oldbuf | 314, 526, 328 | 328 | 1.71 | 1.91 | 13.8, 9.5, 9.7 |
| buf | 456, 649, 388 | 456 | 1.25 | 1.49 | 10.9, 7.9, 9.5 |
| BullMQ | 1,438, 1,903, 923 | 1,438 | 0.29 | 0.45 | 10.4, 7.4, 8.3 |

Paired ratios: new against old 1.17, 1.47, 1.18; buf against oldbuf 1.45, 1.23, 1.18. Every round improved. Runwick with buffered finish is at 0.32 times BullMQ; main was at 0.23 times in the same rounds.

**Memory log (appendfsync always on tmpfs, so a disk sync costs almost nothing): two sets of three rounds**

| Arm | Set 1 jobs/s | Set 2 jobs/s | Median of all six | Script ms per job (set 1, set 2) | CPU ms per job (set 1, set 2) | Load at start |
|---|---|---|---|---|---|---|
| old | 653, 585, 494 | 576, 638, 643 | 614 | 1.44, 1.35 | 1.58, 1.49 | 9 to 12 |
| new | 709, 605, 494 | 778, 726, 777 | 718 | 1.26, 1.07 | 1.40, 1.22 | 9 to 13 |
| oldbuf | 784, 695, 542 | 675, 692, 834 | 694 | 1.34, 1.30 | 1.44, 1.41 | 9 to 11 |
| buf | 662, 877, 536 | 932, 1,003, 935 | 906 | 1.27, 0.94 | 1.40, 1.05 | 8 to 13 |
| BullMQ (set 1 only) | 3,091, 3,282, 2,482 | | 3,091 | 0.22 | 0.30 | 9 to 12 |

Paired ratios, new against old: 1.09, 1.03, 1.00 (set 1) and 1.35, 1.14, 1.21 (set 2). Buf against oldbuf: 0.84, 1.26, 0.99 (set 1) and 1.38, 1.45, 1.12 (set 2). My Redis test suite was running during both sets. The first set does not separate the arms. The second set does, in every round. Across all six rounds the change gives 1.17 times without buffered finish and 1.31 times with it. BullMQ is still about 3.4 times faster with a memory log.

### What is left

- On a quiet host, Runwick's drain still costs about 4 times BullMQ's script time per job (about 1.2 against 0.3 ms) and drains 3 times slower. The remaining cost is spread thinly: each of the phases above is 5 to 25% of the script. There is no single large item left that can be cut without changing what each command stores (intent, receipt, events, indexes).
- The earlier reports' Redis timings were taken under load 17 to 75 and overstate script cost by about three times. Their before-and-after ratios still hold, because both arms of each pair saw the same load. Script time per job is only comparable between runs at similar load.
- No steady or delayed workload was rerun. No quiet-host repeat of the loop-carried branch's full benchmark was done (only one slow-log drain per arm, quoted above).
