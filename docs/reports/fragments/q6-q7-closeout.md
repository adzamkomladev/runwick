# Q6/Q7 and E close-out (SQLite wake, P-08 analysis, allocation test)

**Short answer:** the code parts are done and now tested. The SQLite wake commits first, then wakes. It sends no wake for a replay or a delayed job, a broken listener can't fail an add, and polling still finds work when every wake is dropped. The P-08 analysis fix is in. The allocation test failure was a test bug (a cold call compared with a warm one), not a product regression. The new steady run at 100 offers/s on current code is much better than the old one: 98.7 completions/s, p99 1.24 s, 0 busy conflicts. It still refused 310 of 24,000 offers (1.3%) on a shared host. Some of the measurements the plan asks for during the load are not covered by the harness: crash/reopen, scheduler and outbox checks, and checkpoint progress over time. So the plan item should stay open (`[~]`) for those.

## Requirements

| Requirement | Status | Evidence |
|---|---|---|
| Wake only after commit | Met | `5813e91`. Test `Local_work_hint_crosses_store_instances_after_commit_and_is_best_effort` holds the commit at a barrier and checks that no wake fires and no row is visible. The wake arrives once the commit is released. |
| A failed wake cannot fail an add | Met | The same test registers a listener that throws, and the dispatch still returns Applied. `PublishLocalWake` catches each listener's error separately. |
| No wake on replay | Met | Single dispatch: the same test resends the command, which comes back `Replayed` and adds no second wake. Batch: the new test `Local_work_hint_for_a_batch_names_each_due_queue_once_and_skips_delayed_items_and_replays` covers this. |
| Batch wake: each due queue once, no wake for a not-yet-due job | Met (new test) | Same new test. A batch of two due jobs on `default` and one job delayed an hour on `other` gives exactly one wake, for `default`. A mutation check that ignores `NotBefore` makes the test fail. |
| Polling fallback when wakes are lost | Met | `PollingContractTests.With_every_wake_dropped_polling_finds_work_controls_and_recovery(Sqlite)` (OBS-01) drops every wake and still finds work, a pause and resume, and a dead lease. It now also asserts that the SQLite local wake actually reached the worker, so the test proves the wake was there and was dropped. Before, it would have passed even if the wake had never been wired to the worker. |
| Wake only within one process | Met by design | Listeners are keyed by database file and application, inside one process. Other processes and staged (caller-transaction) writes rely on polling. `ILocalWorkWakeSource` says so in its documentation. |
| P-08 readiness analysis fix | Met | `e3bfd95`, `tests/Runwick.Workloads/Analysis.cs` now counts every applied reserve counter family (`reserve:`, `reserveAndBegin:`, `reserveMany:`). Later P-08 runs report readiness (P-08c: 6.9 s in `reference-20261117-rest`). |
| Core allocation failure reproduced and traced | Met | See below. |
| SQLite steady 100/s, retained history, cleanup, dashboard reads | Partly met | New run on `9a7e66f` (below). Offers are not fully accepted: 1.3% were refused. |
| Writer queue age, refusals, durable completions, pickup p99 | Met (measured) | New run, below. |
| Checkpoint progress | Partly met | Only an after-run WAL snapshot exists: 268 frames, 4.4 MB, not busy. There is no reading over the course of the run. |
| Crash/reopen, scheduler and outbox checks under that load | Not met as a combined run | The harness has no steady mode that runs these together. Separately they pass in the suites: SQLite fault-point kill tests (`A_writer_killed_at_a_fault_point_...`), the scheduler and outbox-source contract rows on SQLite. |

## The allocation test failure

`Prepare_allocation_is_linear_in_payload_size` (SER-12) failed before `e3bfd95`. Putting the old test back on the current tree reproduces it: ratio 2.60 three runs out of three (3.0 was recorded on 14 November). The preparation source between `e3bfd95` and now differs only by `d44cb01`, which touches only the batch cap.

Allocated bytes per stage on this host:

| Stage | 512 KiB first call | 512 KiB warm | 1 MiB first call | 1 MiB warm |
|---|---:|---:|---:|---:|
| Serialize (includes scan) | 4.13 MB | 4.12 MB | 9.16 MB | 9.16 MB |
| Scan only | 2.22 MB | — | 4.44 MB | 4.43 MB |
| Whole Prepare | 7.29 MB | 5.13 MB | 15.42 MB | 11.21 MB |

Serializing and scanning cost the same on the first call as on later ones. The one-time extra cost is in the digest step, after serialization. Renting and returning two 2 MiB arrays from `ArrayPool<byte>.Shared` before the first 1 MiB call cost 4.20 MB, and after that the "cold" 1 MiB Prepare allocated 11.22 MB, the same as a warm one. The first call at each size fills that size's power-of-two pool bucket: 1 MiB arrays for the 512 KiB payload, 2 MiB arrays for the 1 MiB payload. The old test warmed only the 512 KiB size, so it compared a cold 1 MiB call with a warm 512 KiB call. Once warm, the ratio is 2.19 (serialize 2.22×, scan 2.0×), which is under the 2.2 limit, so allocation is linear. The fix in `e3bfd95` warms both sizes. Risk: 2.19 sits close to 2.2, so a small real regression would first show up as a flaky failure.

## SQLite steady run at 100 offers/s

Run on `9a7e66f` (main `391f520` plus the new tests), with the same command as the 2978f04 run plus `--store-viewers 2`. That means 16 callers, 16 worker slots, 1 KiB jobs, 240 s of offers, and a copy of the 5,000-terminal-job retained fixture. SQLite used WAL with synchronous FULL on local ext4. The run held `/tmp/runwick-bench.lock`. Host load was 4.5 to 6.8 during the run (8 cores, other agents active). Raw results are in `artifacts/perf/round-two/sustained-100-q6q7/` (`sqlite.json`, `load.log`, `host.txt`).

| | 2978f04 (before wake) | 9a7e66f (now) |
|---|---:|---:|
| Offered | 24,000 | 24,000 |
| Accepted and completed once | 21,774 | 23,690 |
| Refused (`local_capacity_exceeded`) | 2,226 | 310 |
| Completions/s | 90.6 | 98.7 |
| Offer-to-finish p50 / p99 | — / 5.61 s | 64 ms / 1.24 s |
| Due-to-handler p99 | 3.94 s | 353 ms |
| Busy conflicts | 0 | 0 |
| Writer queue p99, dispatch / cleanup / presence | 543 ms / 8.51 s / 3.81 s | 283 ms / 2.05 s / 2.45 s |
| Store page snapshot reads | none | 96 polls, p99 17 ms |
| WAL after run | 4.4 MB | 4.4 MB (268 frames, not busy) |

The books balance: offered = accepted + refused, and completed = accepted. The due-to-start goal (p99 under 2 s) was met in this run. The rate goal was nearly met: 1.3% of offers were refused at the local admission limit. The two runs were on different days with different host load, so the improvement is not a paired measurement.

## What I added

- `SqliteDispatchGroupingTests.Local_work_hint_for_a_batch_names_each_due_queue_once_and_skips_delayed_items_and_replays`, which covers the batch wake path. Before this, it had no test.
- An OBS-01 assertion for SQLite only: the local wake reaches the worker engine, which then drops it.
- Nothing was broken, so no product code changed.

## Tests

SQLite 55/55, Core 453/453, Contract 496/496 (Redis-backed rows used a fresh AOF-always Redis with the harness profile; the first Contract run failed only its Redis rows because that container lacked `--aof-load-truncated no`).

## What is left

- A steady run that also does crash/reopen, scheduler and outbox work under the same load, and samples checkpoint progress during the run. This needs a harness change.
- A quiet-host repeat to confirm whether the remaining 1.3% of refusals is host load or a real limit.
