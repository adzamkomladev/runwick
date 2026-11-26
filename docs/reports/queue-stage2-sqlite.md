# Stage 2, SQLite: the single writer's transaction body is the cost; the worker is not the limit

26 November 2026 · Runwick v0.1.5 behaviour · SQLite in the Runwick process (WAL, `synchronous=FULL`), file on `/dev/shm` · 8 shared vCPU AMD EPYC · [overview and method](queue-stage2-overview.md) · raw rounds in [`evidence/stage2/sqlite-tmpfs/`](evidence/stage2/sqlite-tmpfs/), text in `summary-sqlite.txt`

SQLite runs inside the Runwick process, so "process CPU" below includes the store's own work; there is no separate store CPU number.

## Numbers

### Single add, one caller (median of 5 rounds, range in brackets)

| Arm | ms per add | Jobs/s | Process CPU ms | Allocated per add |
|---|---:|---:|---:|---:|
| Public `DispatchAsync(job)` | 1.64 (1.32–2.65) | 611 | 1.65 | 87 KiB |
| Preparation alone | 0.056 (0.038–0.076) | | 0.058 | 39 KiB |
| A command prepared before the timer | 1.40 (1.19–1.89) | 714 | 1.41 | 49 KiB |
| The bare store call | 1.27 (1.14–1.42) | 790 | 1.31 | 48 KiB |
| Receipt replay | 0.40 (0.29–0.90) | 2,470 | 0.45 | 12 KiB |

Preparation is 3.4% of a public add (0.056 ms: payload serialization 0.006, validation and intent 0.013, fingerprints 0.032 per job). Runwick's own layer around the store call (public minus prepare minus store call) is 0.31 ms (19%): admission, building the store command, translating the reply. The store call is 78% of the time. A bare `synchronous=FULL` 1 KiB insert on memory runs 21,707/s at one caller ([fast-disk baseline](queue-baseline-v0.1.5-fast-disk.md)), 0.046 ms, so the dispatch call takes about 28 times as long as a bare insert. Replaying a receipt, which does no job insert, still takes 0.40 ms, a rough floor for one call into the store.

One discrepancy to know about: a fresh `bench --mode enqueue --calls 1 --n 2000` process measures 305 to 440 adds/s for SQLite and 249 to 308 for Redis here (three runs each, today), against 611 and 420 in the single-caller arms above. The arms run after a warm-up in a long-lived process and the bench runs include start-up costs, so absolute rates differ by 30 to 40%; compare arms with each other, not with the baseline's single-caller rows.

### Many callers (5 rounds, 3,000 adds per arm; jobs/s)

| Callers | Public | Prepared first | Bare store call |
|---:|---:|---:|---:|
| 16 | 1,050 (984–1,103) | 1,157 (1,020–1,246) | 1,150 (833–1,237) |
| 64 | 1,111 (948–1,116) | 1,103 (1,093–1,260) | 1,173 (900–1,255) |

All three plateau at about 1,100 adds/s from 16 callers on. That is consistent with one writer that callers queue for (not separately proven here). Process CPU is 1.0 to 1.35 ms per add. Preparing first changes the 64-caller rate by 0%.

### Bulk add, 100 jobs per call (median of 5 rounds)

| Arm | ms per call | Jobs/s | Process CPU ms | Allocated |
|---|---:|---:|---:|---:|
| Public `DispatchManyAsync` | 34.3 (30.7–41.5) | 2,920 | 33.8 | 4.7 MB |
| Preparation alone | 4.15 (2.38–5.22) | | 4.3 | 2.7 MB |
| Prepared first | 31.8 (25.9–37.2) | 3,150 | 29.5 | 2.0 MB |
| The bare store call | 31.5 (25.2–34.0) | 3,170 | 29.5 | 2.0 MB |

Preparation is 12% of a public bulk call and 57% of its allocated bytes; the store call is 92% of the time (315 µs per job). Preparing for free would raise bulk by at most about 13%, under the plan's 20% bar.

### Drain, 20,000 ready jobs (jobs/s, median and range)

| 16 slots | Rounds | Jobs/s | Claim size | Finish size | Process CPU per job | p99 handler return to confirm |
|---|---:|---:|---:|---:|---:|---:|
| Public worker | 5 | 462 (445–529) | 8.4 | 18.9 | 2.98 ms | 116 ms |
| Direct driver, same sizes | 5 | 475 (242–518) | 8.0 | 8.5 | 2.98 ms | 88 ms |
| Direct, flush 0 ms / 20 ms | 3 / 3 | 411 / 407 | 8.0 | 8.0 / 17.3 | 3.38 / 3.37 ms | 122 / 168 ms |
| Public, claim ahead 16 | 3 | 459 (369–510) | 16.9 | 28.2 | 2.64 ms | 218 ms |
| Public, 2 claims in flight | 3 | 520 (417–529) | 8.0 | 18.6 | 2.96 ms | 138 ms |

| 64 slots | Rounds | Jobs/s | Claim size | Finish size | Process CPU per job |
|---|---:|---:|---:|---:|---:|
| Public worker | 3 | 492 (305–538) | 32.0 | 31.8 | 2.61 ms |
| Direct driver, same sizes | 3 | 593 (335–635) | 32.0 | 32.0 | 2.39 ms |
| Public, claim ahead 32 | 3 | 574 (463–616) | 32.0 | 32.0 | 2.38 ms |
| Public, 2 claims in flight | 3 | 577 (504–592) | 31.9 | 31.9 | 2.37 ms |

Everything lands between 400 and 600 jobs/s, and the ranges overlap throughout, including the direct driver's single very slow rounds (242 and 202 jobs/s). None of the worker knobs separates from the default beyond that noise, and 16 slots (462) and 64 slots (492) drain at the same speed. The direct driver, which is just claim and finish calls in a loop, runs at the same rate as the public worker, so the worker's coordination is not what holds SQLite down.

## First attributable cost, and what would falsify it

**The writer's transaction body in the SQLite store.** Single adds, bulk adds and drain all sit on the one writer thread: 1.27 ms per single add, 0.32 ms per bulk job, and 2.4 to 3.0 ms of process CPU per drained job (claim, begin, finish and their receipts), with the public worker, the direct driver, every batch size and every slot count landing at the same 400 to 600 jobs/s. Preparation (3% single, 12% bulk) and the worker are ruled out as first costs here.

**What would falsify it:** run the statement-level profile that already exists (`bench --sqlite-profile-statements`) on a drain and on 100-job bulk adds. If the dispatch and claim statements together are less than about 60% of the writer's time, the cost is elsewhere (marshalling in the writer lane, commit and WAL checkpoint). Separately, a minimal transaction that does the same row writes without Runwick's receipts and counters, at one writer, should come out several times faster than 1.27 ms; if it is within 1.3× of it, the cost is the number of rows and indexes, and the next step is the storage layout, not code.

## Next action by the plan's decision table

| Observation | Action |
|---|---|
| Preparation is 3% of a single add and 12% of bulk | No shared-serialization work for singles; bulk preparation is capped at about +13%. |
| Public and direct drains agree at matched sizes at 16 slots (462 vs 475), and at 64 slots differ only within overlapping ranges | No worker engine change indicated. |
| No one-knob change separates beyond noise | No scheduling-policy change indicated for SQLite. |
| Both arms plateau and the SQLite writer is the expensive operation | Stage 3 SQLite producer and Stage 4 SQLite worker are both writer-transaction work. |

## Not checked

SQLite on a real disk with `synchronous=FULL` (here commits cost no disk time, so the real floor is higher), the `synchronous=NORMAL` comparison with Wolverine and Solid Queue, the statement-level profile (the falsifying experiment), multiple lanes, several worker processes, fewer than seven pairs, EventPipe traces. Two slow drain rounds were kept, not discarded: a direct 16-slot run at 242 jobs/s and a flush-0 run at 202 jobs/s. The cause is unknown; other work on this machine is possible, since the idle gate only checks before a run starts.
