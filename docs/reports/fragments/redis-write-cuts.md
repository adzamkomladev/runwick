# Redis worker write cuts

Date: 2026-11-19. Branch `worktree-agent-af18e524270a3265c`.

## Short answer

The worker path now writes about 1,200 fewer bytes to the Redis append-only log per job at a batch of 8: about 1,050 on the claim and 155 on the finish. In the drain benchmark that comes to about 16% (8,300 down to 6,970 bytes per drained job). Most of the saving comes from the claim receipt, which drops from about 1,260 to about 245 bytes per job.

Replies are byte-identical to before. A lost reply still replays to the same lease or the same result. Old and new clients can share one Redis with no migration.

Jobs per second did not change in a way we can measure. The host was heavily loaded (load 6 to 22), and script time per job is within a few percent of main. Fewer bytes did not turn into more throughput on this machine, because Lua CPU, not the log, is the limit.

BullMQ still writes about 2.5 times fewer bytes per job (2.8 KB against 6.9 KB) and drains about 2 to 4 times faster.

## Where the bytes go

Measured with `eng/bench-compare/redis-group/writes.py`: AOF bytes per job by key, for a claim of 8 jobs and a FinishMany of the same 8.

| Key | Claim before | Claim after | FinishMany before | FinishMany after |
|---|---:|---:|---:|---:|
| Receipt (`rcpt:<cmd>`) | 1,263 | 244 | 651 | 495 |
| Job hash | 889 | 862 | 735 | 735 |
| Event row | 576 | 576 | 348 | 348 |
| State index | 113 | 113 | 114 | 114 |
| Lease index | 65 | 65 | 45 | 45 |
| Attempt to job | 84 | 84 | – | – |
| Receipt expiry index | 14 | 14 | 66 | 66 |
| Terminal index | – | – | 66 | 66 |
| Event index | 42 | 42 | 24 | 24 |
| Ready index | 46 | 46 | – | – |
| Meta | 16 | 16 | 16 | 16 |
| MULTI/EXEC | 4 | 4 | 4 | 4 |

Totals per job:

| Command | Batch | Before | After |
|---|---:|---:|---:|
| Claim | 8 | 3,113 | 2,067 |
| FinishMany | 8 | 2,067 | 1,911 |
| Finish (single) | 8 | 2,526 | 2,370 |
| FinishAndClaim | 8 | 5,198 | 3,996 |
| Claim | 1 | 4,031 | 3,128 |
| Finish | 1 | 2,523 | 2,367 |
| FinishAndClaim | 1 | 6,346 | 5,287 |

At a batch of 1, the claim receipt goes from 1,627 to 751 bytes and the finish receipt from 651 to 495.

## What was cut and why it is safe

### 1. Compact claim and finish receipts

A receipt stores the reply so that a resent command (after a lost reply) gets the same answer. Before, it stored the whole reply as JSON, with every key name spelled out for every job.

Now a claim or finish receipt stores only the values, in a fixed order, under a version marker: `{"~":1,"c":[...]}` for a claim and `{"~":1,"f":[...]}` for a finish. Inside a claim, a value that equals the previous job's value is written as `0` (most jobs in a batch share a queue, profile, worker, lease and so on). On replay, the same builder functions that made the original reply rebuild it from these values, so the replied text is byte-for-byte what the first call sent.

Safety rules:

- The stored values are the reply's own values, not values read again from the job. We checked rebuilding the reply from the job hash instead, and it is unsafe: the revision, lease, eligible time and state change after the claim, and retention can delete the job.
- Anything the builders do not cover falls back to storing the plain reply text, as before. That includes blocked, deferred, closed or fenced claim items, an unexpected key, and a value that is not text.
- A receipt with an unknown version marker is never reported as success. Replay returns `UnsupportedVersion` with `receipt_codec_unsupported`, and resolve throws `StoreIncompatibleException`. This follows spec 23 §8 (UPG-07).
- `eng/bench-compare/redis-group/diffcheck.py` runs the old and new scripts side by side, replays each command, and compares every key after expanding receipts. Result: 49 keys compared, 0 differences.

### 2. Empty `code` and `detail` fields left out

Claim and finish receipts no longer write `code=""` and `detail=""`. Readers already treat a missing field as empty.

### 3. Unchanged job fields not rewritten

The job-field writer now skips an HSET when the value is already the same. On the claim path this stops the `unstarted` field from being rewritten with its old value, which saves about 25 bytes per claimed job.

### 4. Quoted-text memo (CPU only)

The JSON quoting helper keeps the quoted form of each string for the rest of the call. A claim repeats ids, states and names, and the compact codec quotes them again when it rebuilds, so this made encoding a claim about 25% faster and paid for the codec's own cost.

### Mixed versions (rolling upgrade)

- The new code replays old plain receipts as they are stored, because plain text is passed through unchanged.
- Old code never replays a new claim or finish receipt. Those commands are resent only by the process that first sent them, and that process runs the same version.
- The old `ResolveOperation` path parses the stored result as JSON and reports "not a dispatch" for a claim, which is what it did before for any non-dispatch receipt. The new one expands the result first.
- Staged completion is PostgreSQL-only, so it is not affected.
- Nothing stored needs a migration. Old receipts expire on their usual schedule.

### Ideas checked and dropped

| Idea | Why not |
|---|---|
| Rebuild the claim reply from the job hash | Unsafe. The job changes after the claim, and retention can delete it. |
| Delete or trim the receipt after the finish | A DEL or HSET adds log bytes instead of saving them. Receipts already live only until NotAfter plus a 10-minute floor. |
| Shorter receipt expiry | Same as above: expiry costs a write, and the floor exists for replay. |
| Drop `"actorId":null` and `"attemptId":null` from event rows | Would save about 76 bytes per job (about 1%). The layout test (`RedisDispatchLayoutTests`) pins every key, and old `EventsSince` readers tolerate it, but it is a layout change. Left for a separate decision. |
| Drop the attempt fields `token` and `closedby` | A layout change. Left alone. |
| A structural codec (key-name table) and a text-substitution codec | Both cut the bytes, but they added about 60 µs of Lua per claimed job. The builder-based codec replaced them. |

## Tests

New file `tests/Runwick.Redis.Tests/RedisCompactReceiptTests.cs` (spec row REDIS-20):

- RC-01 runs the codec directly in Redis Lua: a 4-item claim with awkward texts, an absent timeout after a present one, and repeated values; two finishes; fallbacks for an extra key, a non-text value and a decision without lists; an unknown version giving nil; and plain text passed through unchanged.
- RC-02 checks that claim, Finish and FinishMany write compact receipts, that a resend returns a reply equal to the original, that a reply lost after commit replays correctly, and that `ResolveOperationAsync` expands the result.
- RC-03 writes plain receipts the way the old version did, then has the new scripts replay them for claim, Finish, FinishMany and FinishAndClaim.
- RC-04 plants receipts with version 2 and expects `UnsupportedVersion` / `receipt_codec_unsupported` on replay, `StoreIncompatibleException` from resolve, and the job left as it was.

Full runs against Redis 8.2 with `--appendonly yes --appendfsync always --aof-load-truncated no --maxmemory-policy noeviction --save ''`:

| Suite | First codec (14297cf) | Final codec (319ef10) |
|---|---|---|
| Core | 454 of 454 | 454 of 454 |
| Contract | 496 of 496 | 496 of 496 |
| Redis | 128 of 128 | 128 of 128 (2,466 s) |

## Benchmarks

Paired against main, arms rotated each round, with `pairs-combined.sh` under `flock /tmp/runwick-bench.lock`. The arms are `base` (this branch), `buf` (this branch with buffered finish), `old` and `oldbuf` (main through `BASE_BIN`), and BullMQ. Each log type ran 4 rounds. Host load ranged from 6 to 22, so treat single rounds as noisy and read the medians and paired ratios.

### Drain, final codec

| Log | Arm | Jobs/s per round | Median | Script ms/job | CPU ms/job | AOF bytes/job |
|---|---|---|---:|---:|---:|---:|
| disk | base | 287, 386, 418, 370 | 378 | 1.15 | 1.37 | 6,967 |
| disk | buf | 540, 494, 701, 401 | 517 | 0.98 | 1.12 | 6,827 |
| disk | old | 380, 457, 403, 351 | 391 | 1.12 | 1.30 | 8,298 |
| disk | oldbuf | 419, 414, 666, 535 | 477 | 1.01 | 1.13 | 8,167 |
| disk | BullMQ | 1229, 1605, 1179, 912 | 1,204 | 0.29 | 0.45 | 2,815 |
| tmpfs | base | 580, 701, 820, 580 | 641 | 1.09 | 1.16 | 6,984 |
| tmpfs | buf | 924, 852, 859, 791 | 855 | 0.92 | 0.96 | 6,820 |
| tmpfs | old | 677, 661, 778, 622 | 669 | 1.04 | 1.13 | 8,315 |
| tmpfs | oldbuf | 957, 994, 858, 744 | 907 | 0.87 | 0.92 | 8,158 |
| tmpfs | BullMQ | 2078, 2419, 3640, 3403 | 2,911 | 0.21 | 0.27 | 2,815 |

Paired ratios (new over main, same round):

| Log | Pair | Per round | Median |
|---|---|---|---:|
| disk | base / old | 0.75, 0.84, 1.04, 1.06 | 0.94 |
| disk | buf / oldbuf | 1.29, 1.19, 1.05, 0.75 | 1.12 |
| tmpfs | base / old | 0.86, 1.06, 1.05, 0.93 | 0.99 |
| tmpfs | buf / oldbuf | 0.97, 0.86, 1.00, 1.06 | 0.98 |

The bytes drop by 16% in every arm. Throughput moves both ways from round to round and the medians sit within noise of 1.0.

The first (structural) codec's run, before it was replaced, showed the same byte cut (8,311 down to 7,065 on disk) and the same pattern of throughput within noise, but with script time about 10% higher. That led to the builder codec.

### In-script CPU

`profile_worker.py`, rotated, median µs per job:

| Batch | Step | Main | This branch |
|---:|---|---:|---:|
| 8 | Claim | 346 | 340 |
| 8 | FinishMany | 275 | 263 |
| 8 | Both | 508 | 503 |
| 1 | Claim | 1,060 | 1,060 |
| 1 | Finish | 753 | 788 |

The k=1 finish difference is within this host's run-to-run noise. The variants without the memo or without compact receipts measured 750 and 762.

### Delayed jobs (delayed100, 2 rounds, means)

| Measure | main | this branch | BullMQ |
|---|---:|---:|---:|
| Completed per second | 100 | 100 | 98.4 |
| Pickup p95 (ms) | 142 | 106 | 256 |
| Pickup p99 (ms) | 339 | 225 | 414 |
| Enqueue p99 (ms) | 255 | 151 | 177 |
| Offer to finish p99 (ms) | 5,375 | 5,291 | – |

Delayed pickup is not worse.

## What's left

- **Event rows** are the next biggest write after the job hash (576 bytes per claimed job, 348 per finished one). Dropping null `actorId` and `attemptId` would save about 76 bytes per job. That needs a decision on the layout test and on outside readers.
- **The job hash** (about 860 and 735 bytes) is mostly field names and fresh values. Shorter field names would be a real layout migration.
- **Attempt fields** `token` and `closedby` could go, but that is also a layout change.
- **The gap to BullMQ** is about 4 KB per job and about 0.8 ms of Lua per job. Byte cuts alone will not close the throughput gap on this host. Lua CPU per job is the limit, so the next round should target script work (fewer index updates, fewer events per step) rather than log bytes.
- **summarize_steady.py** crashes in its ratio section on delayed100 results (`KeyError: 'off'`). The per-metric lines print fine.
