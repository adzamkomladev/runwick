# Redis: lean worker scripts (perf round two, Redis F)

## Short answer

The scripts a Redis worker runs to claim, finish and renew jobs now have a hand-written fast path for the usual case. Nothing stored changed: job hashes, receipts, event rows, AOF bytes and every reply, replays included, are byte for byte what main writes. No migration, and old and new binaries can run side by side.

Script CPU per job fell by about a quarter. A claim at eight jobs per call went from 270–278 to 196–207 µs per job, and a FinishMany from 250–259 to 193–199 µs.

On this shared host that saving did not show up as faster drains. Paired against main with buffered finish, the drain rate was 0.90× on the disk log and 0.96× with the AOF on memory (medians of four rounds, with wide spread). BullMQ is still about two to three times faster. The 90% target is not met by this change alone. The stored-data cuts that build on it (layout revision 2, docs/reports/fragments/redis-layout-2.md) are what move the drain rate.

## What changed and why it is safe

Each of `ReserveAndBegin`, `Finish`, `FinishMany`, `Renew` and `RenewMany` now runs in two layers inside one script:

- **The generic steps** (gates, receipt lookup, the intent, recovery) are defined as one function, `GENERIC()`, but not run.
- **The lean path** runs first and only reads. As soon as it sees anything unusual, it calls `GENERIC()` before it has written anything, so the generic steps decide the command from the start exactly as before. Unusual means a gate or admission refusal, a pending guard, a receipt to replay, a deadline problem, a paused or missing queue, concurrency or rate scopes, a cancel request, a refused ownership, a duplicate job or command in one call, more than 64 items, large finish bodies, or a counter at its limit.
- **When the lean path decides**, it writes the after-image the generic steps would write: the same keys, fields, members, receipt and event texts, and reply. It writes them as one direct projection under the existing crash rule. If a write fails partway, the intent is stored with `ipos` 1 and recovery finishes it, as for any other command.

Most of the old cost was the generic machinery itself (defining and walking hundreds of helpers per call), not the data written. That is the part the lean path skips.

## Tests

- `eng/bench-compare/redis-group/diffcheck.py` runs main's scripts and these scripts on the same data: dispatch, claim, renew, finish, FinishMany and FinishAndClaim, every call sent twice so replays are compared, plus fenced, blocked, cancelled and exhausted claims, a uniqueness key, usage and error texts and a retry. Every key and every reply is byte-identical.
- New `RedisLeanScriptTests` (REDIS-22):
  - LS-01: the lean helpers (claim item, finish result, compact receipt, quoting, byte order, counters) give exactly what the generic helpers give, for usual and awkward texts.
  - LS-02: claim, Renew, RenewMany, Finish, FinishMany and a claim that finds no work take the lean path and never call the generic one. A replayed claim does call it.
  - LS-03: every write prefix of a lean claim, RenewMany, Finish and FinishMany (the FailAfter seam) leaves either a stored intent or a lost reply. The resend recovers it and answers as the first call would, each job finishes once, and the store's invariants hold.
- `RedisCompactReceiptTests` RC-03, which simulates an older binary, now forces that binary onto the generic path.
- Full suites against Redis 8.2 with `appendfsync always`, built from this commit alone:
  - Runwick.Redis.Tests: 133 of 134 passed. The one failure was `RedisSentinelTests`, which timed out waiting for its replicas to sync during test setup, before any Runwick write. Rerun alone, the Sentinel class passed 8 of 8.
  - Runwick.Core.Tests: 455 of 455 passed.
  - Runwick.Contract.Tests: 502 of 502 passed.
- An earlier run failed the kill -9 crash test (`RedisCrashTests`, REDIS-01) for Finish and Cancel. That run had host load averages of 40 to 52. The resend arrived after the command's 60-second deadline, and the client correctly gave up with `store_recovering`. Cancel never uses a lean script, which pointed at the host. Rerun at load averages of 15 to 18, both cases passed, as did all other crash cases. Main passed the same cases at similar load.

## Benchmarks

### Script CPU per job

Measured with `profile_worker.py --plain`, which loads the scripts as the binary composes them and times them with Lua's process CPU clock (not distorted by other load), on a Redis with the AOF on. Two rounds each. µs per job:

| Path | Jobs per call | main | this change |
|---|---:|---:|---:|
| Claim (ReserveAndBegin) | 1 | 764–826 | 585–650 |
| Finish (FinishMany) | 1 | 680–698 | 502–509 |
| Claim (ReserveAndBegin) | 8 | 270–278 | 196–207 |
| Finish (FinishMany) | 8 | 250–259 | 193–199 |

### Drains

Each run drained 20,000 jobs with 16 workers on a fresh Redis 8.2 (`appendfsync always`). Arms ran in rotating order within each round, and each round held the shared bench lock. All arms here use buffered finish: `oldbuf` is main, `midbuf` is this change. The host was shared, with load averages of 7 to 25, so read the paired ratios, not single numbers.

| Arm | Disk: jobs/s per round | median | script ms/job | AOF B/job |
|---|---|---:|---:|---:|
| oldbuf (main) | 464, 545, 475, 464 | 470 | 0.93 | 6,829 |
| midbuf (this change) | 328, 392, 582, 497 | 445 | 0.87 | 6,830 |
| BullMQ | 1236, 1232, 870, 501 | 1,051 | 0.34 | 2,815 |

| Arm | AOF on memory: jobs/s per round | median | script ms/job | AOF B/job |
|---|---|---:|---:|---:|
| oldbuf (main) | 912, 1163, 994, 994 | 994 | 0.74 | 6,818 |
| midbuf (this change) | 1064, 1228, 861, 728 | 962 | 0.71 | 6,818 |
| BullMQ | 3112, 3235, 3461, 3942 | 3,348 | 0.21 | 2,816 |

| Paired ratio | Disk, per round | median | Memory, per round | median |
|---|---|---:|---|---:|
| midbuf / oldbuf | 0.71, 0.72, 1.22, 1.07 | 0.90 | 1.17, 1.06, 0.87, 0.73 | 0.96 |
| midbuf / BullMQ | 0.27, 0.32, 0.67, 0.99 | 0.49 | 0.34, 0.38, 0.25, 0.18 | 0.30 |
| oldbuf / BullMQ | 0.38, 0.44, 0.55, 0.93 | 0.49 | 0.29, 0.36, 0.29, 0.25 | 0.29 |

AOF bytes per job are unchanged, as expected. Redis's own script time per job fell only 4% to 7% in the drains, much less than the profile's quarter. The two measure different things. The profile counts Lua CPU in the claim and finish scripts alone. The drain figure is Redis's wall time across every script call, including the dispatches that insert the jobs, which this change does not touch.

## What's left

- **The drain gain is not measurable on this host.** The CPU saving is real in the profile, but the round-to-round spread (0.71 to 1.22) is larger than the effect. A quiet host would be needed to see it, if it is there.
- **The 90% target is far off.** What is left is the data written per job (receipts, attempt records, event rows), which the layout revision 2 cuts address.
