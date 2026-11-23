# Redis: storing less per job (layout revision 2)

## Short answer

A claimed and finished job now writes about a quarter fewer bytes to Redis. The cuts come from docs/plans/redis-stored-data-design.md (options a, b, c and e). Replies, management reads and the `/v1/events` feed are unchanged, byte for byte.

Paired against main with buffered finish, drains are 1.19× faster on the disk log and 1.21× with the AOF on memory (medians of four rounds). Without buffered finish they are 1.00× on disk and 1.44× on memory. AOF bytes per drained job fell from about 6,830 to about 5,050.

Runwick with buffered finish now reaches 0.59× of BullMQ's drain rate on disk (main: 0.49×) and 0.36× on memory (main: 0.29×). The 90% target is not met.

Upgrading needs the usual layout steps: drain, stop, migrate, start. No data is rewritten, and data written by the old layout stays readable.

## What changed and why it is safe

- **One finish receipt per call (option a).** The finishes first sent in one FinishMany or FinishAndClaim call now share one receipt hash, `rcpt:<call>:f`, with one expiry entry. Before, each finished job had its own nine-field hash and its own expiry entry. Each field still holds that job's fingerprint, deadline, decision time and result, so a resend still replays the exact reply. When the process resends a finish, it names the call that first carried it (the client remembers this until the finish's deadline plus 30 seconds), so the script finds the receipt in one lookup. A finish resent without that hint, or decided in a later call, gets an ordinary per-job receipt, as before. This is a new receipt form, not a layout change.
- **Attempt records packed at close (option c).** While an attempt is open, nothing is written under `a:<attempt>`: the job's own fields plus two new ones, `ares` (reserved at) and `aauth` (authorized at), describe it. When the attempt closes, one field `a:<attempt>` holds the whole record as a short JSON array. The copies of the ownership token and the closing command that nothing ever read are no longer stored. Errors and usage stay in their own fields. An attempt opened under layout 1 closes in the layout-1 form, so a store never mixes forms within one attempt.
- **Events packed per call (option b).** A call's events are stored as one entry with a short head line and one line per event. A claim's reserved and started events for one attempt fold into one line. Event IDs stay consecutive. `EventsSince` expands the entries into exactly the rows it returned before, byte for byte, including paging that starts or stops inside an entry. If any value would not split cleanly, the call falls back to the old one-row-per-event form.
- **No stored `null` (option e).** Event rows no longer store `actorId` or `attemptId` when they are absent; readers already treated a missing field as null.

Options b, c and e change what readers must understand, so they come together as layout revision 2:

- The upgrade is drain, stop, migrate, start (spec 23).
- `migrate` raises each application's layout first and refuses with `intent_pending` if a command is half-applied, then raises the store record.
- A layout-1 binary refuses to run against a layout-2 store, as for any layout change. The new binary also refuses a store that has not been migrated yet (L2-05).
- No data is rewritten. Every reader understands both forms indefinitely.

## Tests

- `diffcheck.py`, lean path against generic path, both in layout 2: 74 keys compared, 0 differences.
- `diffcheck.py --replies-only` against main: every reply, every replay and the `EventsSince` feed read at page sizes 1 to 6 are byte-identical (0 differences). The stored keys differ by design.
- New `RedisLayout2Tests`:
  - L2-01: per-call finish receipts: after a lost reply, every resend (alone, in another batch, or the whole call again) replays the same answer; a changed body is a conflict; another process's resend applies nothing twice.
  - L2-02: event packs: the feed is the same rows at every page size from 1 to 12, and from a cursor in the middle of a pack.
  - L2-03: layout-1 data mixed with new data, read through `GetJob` and events.
  - L2-04: a layout-1 open attempt closes in its own form.
  - L2-05: migrate from layout 1, with the `intent_pending` refusal.
- Tests updated deliberately for the new layout:
  - `RedisDispatchLayoutTests` (the event-pack shape);
  - `RedisScriptCutTests` SC-02 (markers now live in the call's receipt);
  - `RedisControlTests` REDIS-12 (cleanup removes whole packs);
  - `RedisCrashTests`, which now ignores the resend's confirmation time in the call's receipt (`c:<job>` fields), as it already ignored `confirmed` in a per-job receipt;
  - the CLI test, which expects layout revision 2;
  - the harness snapshot and admin readers, which read both forms.
- Full suites against Redis 8.2 with `appendfsync always`:
  - Runwick.Redis.Tests: 138 of 139 passed. The failure was the kill -9 crash test for FinishMany: the recovered state differed from the clean run only in the confirmation time written by the resend. After the test fix above, the FinishMany and Finish crash cases pass. The whole Redis suite was not rerun after that fix.
  - Runwick.Core.Tests: 455 of 455 passed.
  - Runwick.Contract.Tests: 502 of 502 passed.
  - Runwick.Cli.Tests: 20 of 20 passed after the layout-revision fix (19 of 20 before).

## Benchmarks

### Bytes written per job

Measured with `writes.py` at eight jobs per call: AOF bytes per job for each call. The lean scripts alone (the previous change) write exactly what main writes.

| Call | main | layout 2 |
|---|---:|---:|
| Claim | 2,067 | 1,146 |
| FinishMany | 1,911 | 1,288 |
| Finish (single) | 2,370 | 2,078 |
| Dispatch | 3,587 | 3,491 |
| FinishAndClaim | 3,996 | 2,431 |
| RenewMany | 182 | 182 |

### Script CPU per job

`profile_worker.py --plain`, two rounds, µs per job. "Lean" is the previous change alone.

| Path | Jobs per call | main | lean | layout 2 |
|---|---:|---:|---:|---:|
| Claim | 1 | 764–826 | 585–650 | 532–648 |
| Finish | 1 | 680–698 | 502–509 | 516–530 |
| Claim | 8 | 270–278 | 196–207 | 185–199 |
| Finish | 8 | 250–259 | 193–199 | 172–186 |

### Drains

20,000 jobs, 16 workers, a fresh Redis 8.2 per run (`appendfsync always`), arms in rotating order, each round under the shared bench lock. `old` and `oldbuf` are main without and with buffered finish; `base` and `buf` are this change without and with buffered finish. Load averages were 10 to 25 on disk and 7 to 20 on memory, so the paired ratios are the numbers to trust.

Disk log:

| Arm | jobs/s per round | median | script ms/job | AOF B/job |
|---|---|---:|---:|---:|
| old (main) | 294, 416, 366, 340 | 353 | 1.08 | 6,963 |
| base | 301, 390, 359, 416 | 374 | 0.84 | 5,255 |
| oldbuf (main) | 464, 545, 475, 464 | 470 | 0.93 | 6,829 |
| buf | 614, 734, 505, 334 | 559 | 0.71 | 5,059 |
| BullMQ | 1236, 1232, 870, 501 | 1,051 | 0.34 | 2,815 |

| Paired ratio | per round | median |
|---|---|---:|
| base / old | 1.02, 0.94, 0.98, 1.22 | 1.00 |
| buf / oldbuf | 1.32, 1.35, 1.06, 0.72 | 1.19 |
| buf / BullMQ | 0.50, 0.60, 0.58, 0.67 | 0.59 |
| oldbuf / BullMQ | 0.38, 0.44, 0.55, 0.93 | 0.49 |
| base / BullMQ | 0.24, 0.32, 0.41, 0.83 | 0.36 |

AOF on memory (`FSYNC=tmpfs`):

| Arm | jobs/s per round | median | script ms/job | AOF B/job |
|---|---|---:|---:|---:|
| old (main) | 558, 896, 815, 641 | 728 | 0.95 | 7,000 |
| base | 957, 1191, 952, 993 | 975 | 0.64 | 5,325 |
| oldbuf (main) | 912, 1163, 994, 994 | 994 | 0.74 | 6,818 |
| buf | 1020, 1278, 1297, 1380 | 1,288 | 0.53 | 5,046 |
| BullMQ | 3112, 3235, 3461, 3942 | 3,348 | 0.21 | 2,816 |

| Paired ratio | per round | median |
|---|---|---:|
| base / old | 1.72, 1.33, 1.17, 1.55 | 1.44 |
| buf / oldbuf | 1.12, 1.10, 1.31, 1.39 | 1.21 |
| buf / BullMQ | 0.33, 0.40, 0.37, 0.35 | 0.36 |
| oldbuf / BullMQ | 0.29, 0.36, 0.29, 0.25 | 0.29 |
| base / BullMQ | 0.31, 0.37, 0.28, 0.25 | 0.29 |

Without buffered finish, the disk-log drain waits on fsync rather than on scripts, so fewer bytes do not show there.

### Delayed pickup (delayed100)

One round: 100 jobs/s, each delayed 5 s, for 30 s after a 12 s warm-up. Pickup is the time from when a job falls due to when its handler starts.

| | main | layout 2 | BullMQ |
|---|---:|---:|---:|
| Pickup p95 (ms) | 256 | 152 | 280 |
| Pickup p99 (ms) | 450 | 547 | 412 |
| Enqueue p99 (ms) | 315 | 94 | 122 |

With one round this is within noise. There is no sign that pickup got worse.

## What's left

- **The 90% target is not met.** BullMQ uses about 0.2 to 0.3 ms of script time per job; Runwick now uses 0.5 to 0.7 ms. What is left per job is the claim receipt, the state indexes and the fixed cost of each call.
- **Single Finish barely moved** (2,370 → 2,078 bytes): a lone finish still gets its own receipt. Batching via FinishMany or buffered finish is where the cut applies.
- **The whole Redis suite was not rerun** after the crash-test fix; the affected crash cases were.
