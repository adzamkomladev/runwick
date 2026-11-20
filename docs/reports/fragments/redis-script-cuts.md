# Redis: worker script cuts (perf round two, Redis F)

## Short answer

The Lua scripts a Redis worker runs to claim and finish jobs now do about a fifth less work per job. Nothing stored changed: job hashes, event rows, receipts, AOF bytes and every reply, including replays after a lost reply, are byte for byte what main writes. No migration, and old and new binaries can run side by side.

On a disk log, paired against main, buffered-finish drains are 1.20× faster (median of four rounds). With the AOF on memory, both arms gain 1.16×. Redis script time per drained job fell 12% to 18%.

BullMQ is still about three times faster. Runwick with buffered finish reaches 0.39× of BullMQ's drain rate on disk (main: 0.33×) and 0.33× on memory (main: 0.29×). The 90% target is not met. The cost that remains is the work Runwick does per job and BullMQ does not: receipts for exact replay, attempt records, event rows and the state indexes.

## Where the script time goes

Measured with `eng/bench-compare/redis-group/profile_worker.py`, which times each phase inside the script on a quiet Redis. The figures are CPU microseconds per job at eight jobs per call.

| Path | main | this change |
|---|---:|---:|
| Claim (ReserveAndBegin) | 403–422 | 313–342 |
| Finish (FinishMany) | 305–349 | 270–288 |
| Claim + finish | 672–801 | 506–574 |

At sixteen jobs per call, the claim went from 327–374 to 314–334 µs and the finish from 272–288 to 245–247 µs.

These are the claim phases that moved, in µs per call of eight jobs:

| Phase | main | this change | What it is |
|---|---:|---:|---|
| Reply encoding | 516 | 10 | Turning the claimed jobs into the JSON reply |
| Events | 216 | 63 | Building the reserved and started event rows |
| Size bound | 104 | 10 | Upper bound of the packed intent size (§4.3) |
| Payloads | 144 | 58 | Reading each claimed job's headers and created time |

The time that is left is mostly fixed per call, about 150 µs before any job is touched: setup, reading `meta` and the receipt lookup. After that come the index updates and receipts for each job. Redis commands themselves are only about 11% of script time; the rest is Lua work. Lua garbage collection does not matter (pausing it changed nothing).

Redis commands per claimed job went from 7.1 to 6.2. BullMQ's finish-and-fetch is about 25 commands per job, but each is cheap: it has no receipts, no attempt records, no state index by created time, one event instead of three, and it replies with a raw `HGETALL` instead of JSON.

## What was cut and why it is safe

- **Writing JSON texts directly.** The claim item, the finish result, the FinishMany item reply and the details of the `attempt_reserved`, `attempt_started` and attempt-closing events used to be built as Lua tables and run through the generic encoder, which sorts keys and checks every value's type. When the values have their usual types, they are now written as text in the order the encoder would give. Any other value falls back to the encoder, so the output is the same either way. Test SC-01 compares each writer with the encoder on usual and awkward values: quotes, control bytes, non-ASCII, raw JSON, missing fields and wrong types.
- **One encode per reply.** A claim's reply text is built once and reused for its receipt and for FinishAndClaim's claim receipt. Before, it was encoded up to three times. Compact receipts (REDIS-20) rebuild replays with the same writers, so a replay gives the same bytes.
- **No second pass for the size bound.** The packed-size upper bound is summed while the writes are listed. The existing DP-04 test still checks that the bound is never below the real packed size.
- **No second read of the job.** A claimed job's payload fields come from the job hash the claim had already read, not from another `HMGET`. The first page of candidates asks for the claim size plus four instead of 100.
- **No full sort in FinishMany.** Receipts are checked in item order. Only the confirmations written for replayed items are sorted, using the byte comparator (REDIS-11).
- **Fewer lookups.** Writes to a job's fields and to the event rows find their write record once per call, not once per field. The key list of a key set is built only the first time that set is seen.

Not changed: the job-hash and event-row layouts, receipts, the intent format, direct projection and recovery. Because the stored data is identical, the AOF/fsync profile is unchanged too.

## Tests

- `eng/bench-compare/redis-group/diffcheck.py` runs main's scripts and these scripts side by side on the same data. It now also covers a mixed claim (applied with a fencing scope, capacity blocked, invalid state, budget exhausted), a single reserve, a begin and a finish, each sent twice so the replay is compared. All 75 keys and every reply are byte-identical.
- New `RedisScriptCutTests` (REDIS-21):
  - SC-01: the writers against the encoder.
  - SC-02: a FinishMany with items out of order and items repeated from an earlier batch. Only the repeats replay and get confirmed, and every state is right.
  - SC-03: a claim whose reply is lost replays exactly. The claim is followed by finishes of every outcome (succeeded, permanent failure, retry with a delay, cancelled), and all 16 event rows hold canonical details with the expected keys.

  A deliberate key-name change in a writer was checked to fail both SC-01 and SC-03.
- Full suites against Redis 8.2 with `appendfsync always`:
  - Runwick.Redis.Tests: 131 of 131 passed.
  - Runwick.Core.Tests: 455 of 455 passed.
  - Runwick.Contract.Tests: 501 of 502 passed. The one failure was `A_cpu_burning_allocating_handler_records_plausible_usage(Sqlite)`, a CPU-time check on SQLite that this change does not touch, run while the host load was high. It passed when rerun alone on a quiet host (load 3.5).

## Benchmarks

Each run drained 20,000 jobs with 16 workers on a fresh Redis 8.2 (`appendfsync always`, AOF on). Arms ran in rotating order inside one round, and every round held the shared bench lock. `base` and `buf` are this change without and with buffered finish; `old` and `oldbuf` are main. Script ms/job is Redis's own wall time inside scripts, divided by jobs. The host was shared, with load averages between 4 and 36, so the paired ratios within a round are the numbers to trust.

### Disk log (fsync about 7 ms)

| Arm | jobs/s per round | median | script ms/job | AOF B/job |
|---|---|---:|---:|---:|
| old (main) | 334, 370, 302, 208 | 318 | 1.36 | 6,981 |
| base | 326, 289, 329, 318 | 322 | 1.20 | 6,978 |
| oldbuf (main) | 525, 486, 345, 344 | 415 | 1.17 | 6,832 |
| buf | 623, 593, 386, 435 | 514 | 0.96 | 6,827 |
| BullMQ | 1569, 762, 1048, 1117 | 1,082 | 0.35 | 2,815 |

| Paired ratio | per round | median |
|---|---|---:|
| base / old | 0.98, 0.78, 1.09, 1.53 | 1.03 |
| buf / oldbuf | 1.19, 1.22, 1.12, 1.27 | 1.20 |
| buf / BullMQ | 0.40, 0.78, 0.37, 0.39 | 0.39 |
| oldbuf / BullMQ | 0.33, 0.64, 0.33, 0.31 | 0.33 |

Without buffered finish, the disk-log drain is limited by fsync waits rather than script time, so the cut does not show there.

### AOF on memory (FSYNC=tmpfs)

| Arm | jobs/s per round | median | script ms/job | AOF B/job |
|---|---|---:|---:|---:|
| old (main) | 485, 464, 409, 439 | 452 | 1.47 | 6,984 |
| base | 488, 548, 466, 526 | 507 | 1.30 | 6,988 |
| oldbuf (main) | 572, 609, 544, 605 | 588 | 1.27 | 6,823 |
| buf | 786, 659, 673, 584 | 666 | 1.10 | 6,821 |
| BullMQ | 1826, 2155, 1861, 2223 | 2,008 | 0.30 | 2,815 |

| Paired ratio | per round | median |
|---|---|---:|
| base / old | 1.01, 1.18, 1.14, 1.20 | 1.16 |
| buf / oldbuf | 1.37, 1.08, 1.24, 0.96 | 1.16 |
| buf / BullMQ | 0.43, 0.31, 0.36, 0.26 | 0.33 |
| oldbuf / BullMQ | 0.31, 0.28, 0.29, 0.27 | 0.29 |

AOF bytes per job are unchanged, as expected, since nothing stored changed.

### Delayed pickup (delayed100)

Same arms (old is main, base is this change, no buffered finish) and BullMQ, four rounds, 100 jobs/s each delayed 5 s, for 30 s after a 12 s warm-up. Pickup is the time from when a job falls due to when its handler starts (for BullMQ, its own delayed latency). Load averages ranged from 2.6 to 10.5.

| Round | old p95 / p99 (ms) | base p95 / p99 (ms) | BullMQ p95 / p99 (ms) |
|---|---|---|---|
| 1 | 193 / 342 | 328 / 569 | 356 / 639 |
| 2 | 788 / 1,166 | 488 / 664 | 252 / 409 |
| 3 | 497 / 775 | 377 / 566 | 241 / 387 |
| 4 | 3,258 / 3,734 | 450 / 683 | 610 / 788 |
| Median | 643 / 971 | 414 / 617 | 304 / 524 |

Pickup is not worse: base won three rounds of four, and its median p99 is 617 ms against main's 971 ms. It is within about 18% of BullMQ on p99, not within the 10% target. Enqueue p99 was noisy, with base worse in two rounds, equal in one and better in one (median 372 ms against 305 ms). The dispatch path shares only the prelude changes (the size bound and the event and job write records), so this is most likely host noise, but it was not rerun.

## What's left

- **The 90% target is far off.** BullMQ uses about 0.3 ms of script time per job, against Runwick's 1.0 to 1.1 ms. What remains is the model, not encoding: about 150 µs fixed per call (setup, `meta`, receipt lookup), then per job the receipt, the attempt record, three event rows and the state indexes. Closing the gap means doing less of that per job, which is a design decision. Options include batching receipts per call instead of per job, or one event row per step instead of three. Both change stored data.
- **Smaller cuts looked at and not made:**
  - reading only the needed job fields (`HMGET`) instead of `HGETALL`;
  - skipping `ZREM`s that remove nothing;
  - caching the per-call setup;
  - restructuring compact receipts;
  - reading `meta` with fewer fields.

  Each looked too small to pay for its risk, which for some was to mixed versions.
- The event-row null fields and the job-hash field names still need a layout decision (unchanged from the write-cuts report).
