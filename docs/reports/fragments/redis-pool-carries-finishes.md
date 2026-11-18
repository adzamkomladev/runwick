# Redis: the pool's claim carries the waiting finishes

Written 2026-11-18. Informational. Measured on the shared host while other sessions ran (load average 17 to 48 on 8 cores). Arm order rotated each round; the numbers show direction, not exact gains.

## Short answer

Built, tested and safe, but it does not pay off. With buffered finish on, the pool's claim now carries the finishes that are waiting, about 55 % of all finishes on a disk log and 80 % on a memory log. Calls went down only a little, and on a disk log (the normal production setting) the worker got slower than buffered finish alone in all three rounds (0.70 to 0.83 times). On a memory log it came out ahead of buffered finish, but those rounds were noisier. BullMQ stayed 2.5 to 4.5 times faster. My recommendation is to keep this on its branch and not merge it.

## What was built

- **One switch, both directions.** `UseCombinedFinishClaim()` now means "whichever side is ready first carries the other". There is no new option, and the default is still off. A finisher that still holds its slot attaches a claim to its finish, as before. The new part: when the pool's claim loop is about to send a batched claim for its free slots, it first takes the finishes that are waiting and sends them and the claim in one `finish_and_claim` call. The waiting finishes are the batch a buffered-finish collector is filling, oldest first, then whatever is still in the ready channel.
- **Each finish is taken exactly once.** The collector's batch is now shared under the buffer lock, so the loop and the collector cannot both take the same finish. Finishes still in the channel are removed by the channel's own read. If the loop takes the collector's whole batch, the collector goes back to waiting. A test that stops the loop from removing what it takes fails at once.
- **No finish waits longer because of the loop.** The collector still flushes whatever is left at the flush interval, when its batch is full, at drain, or near a lease margin. The loop can only send finishes earlier.
- **Slots.** The claim fills the loop's own free slots, so no slot moves between the two halves of the call. Finishes the call decided complete at once. Any other finish is resolved by its own command in the background, so the loop does not wait for it.
- **Failure handling is the same as before.** A refused call turns the option off for the worker. Each finish then resolves alone, and the loop sends the same claim command alone. If a call's reply is lost, each finish resolves by its own command, and the claim is resent with the same command ID. That resend happens even if admission closed in the meantime, because the claim may already be committed.
- **No stored layout change, no script change, no migration.** PostgreSQL and SQLite are unaffected, because the call stays an optional interface.
- **Bench output** gains `loopCarriedCalls` and `loopCarriedFinishes`.

Spec: 05 Changes 2026-11-18, 22 Changes. Public docs: the configuration page, with the docs index regenerated.

## Tests

| Run | Result |
|---|---|
| Core (fake store), including 6 new `LoopCarriedFinishTests` | 459 of 459 |
| `RedisFinishAndClaimTests`, including new FC-08 (the claim's size does not depend on how many finishes ride with it) | 8 of 8 |
| `FinishAndClaimContractTests`, including new FC-13 (all stores, buffered and combined), FC-14 (lost reply of a carried claim) and FC-15 (refused carried claim) | 10 of 10 |
| Full Redis project | see the final hand-off |

The new Core cases cover five behaviours:

- The loop's claim carries the waiting finishes.
- A 300-job race between the loop and the completion lane sends each finish command exactly once.
- A drain with nothing left to claim still sends the buffered finishes.
- A refused call turns the option off, and the same claim is then sent alone.
- A lost reply resolves each finish by its own command and replays the claim.

A sixth case checks that nothing changes when the option is off.

## Measured: drain of 20,000 jobs (1 KiB, 16 workers, fresh Redis 8.2 per run)

Throughput is in jobs per second. "Scripts per job" counts every Redis script call in the run, including the insert and the readiness probes, divided by 20,000. "Script time" is Redis's own script execution time per job.

**Disk log (appendfsync always on the host disk)**

| Round (load) | base | buf | both (new) | BullMQ | both ÷ buf |
|---|---|---|---|---|---|
| 1 (48 / 26 / 25 / 29) | 62 | 87 | 72 | 428 | 0.83 |
| 2 (31 / 17 / 21 / 39) | 72 | 108 | 83 | 268 | 0.77 |
| 3 (22 / 34 / 23 / 25) | 75 | 122 | 85 | 265 | 0.70 |
| Median | 72 | 108 | 83 | 268 | |

| Disk log, per arm (median) | base | buf | both |
|---|---|---|---|
| Scripts per job | 0.40 | 0.23 | 0.22 |
| Worker claim calls | 2,981 | 2,368 | 1,336 + 1,006 carried |
| FinishMany calls | 1,556 | 1,409 | 789 |
| Finishes carried by the loop | 0 | 0 | 11,444 (57 %) |
| Redis script time per job | 5.1 ms | 4.1 ms | 5.2 ms |

**Memory log (fsync almost free)**

| Round (load) | base | buf | both (new) | BullMQ | both ÷ buf |
|---|---|---|---|---|---|
| 1 (34 / 22 / 45 / 22) | 154 | 123 | 160 | 639 | 1.31 |
| 2 (22 / 29 / 32 / 28) | 193 | 128 | 158 | 992 | 1.23 |
| 3 (20 / 38 / 28 / 26) | 132 | 33 (outlier) | 143 | 723 | 4.3 (outlier) |
| Median | 154 | 123 | 158 | 723 | |

On a memory log, scripts per job were 0.42 for base, 0.22 for buf and 0.19 for both. The loop carried about 16,000 of the 20,000 finishes (80 %).

Raw results are in `eng/bench-compare/redis-group/results/drain-loop` and `drain-loop-tmpfs` in the worktree. They are not committed.

## Why it does not help

- **The mechanism works, but it saves few calls.** Against buffered finish alone, the worker's claim and finish calls fell by only about 15 % (round 1 on disk: 3,765 down to 3,200, finishes sent alone not counted), and all script calls by about 5 %. Buffered finish had already merged most finishes into FinishMany calls.
- **Fewer calls do not mean fewer disk syncs.** Redis syncs the log once per event-loop pass for every client that wrote in that pass. Separate claim and finish calls that arrive together already share one sync, so merging them saves little disk time.
- **The combined call costs more Redis time.** Script time per job rose from about 4.1 ms to 5.2 ms on disk, back to the level without buffered finish. The claim now waits for the finishes in its call, so new jobs start later and slots sit empty longer.
- **On a memory log** disk syncs cost nothing, so fewer calls is a small win. The new path beat buffered finish there, but buffered finish alone was slower than the baseline in those rounds, which points to noise. I do not read a gain from it.

## What is left

- Find out why `finish_and_claim` costs more script time than a FinishMany call plus a separate claim. That would need a profile of the script.
- The main gap to BullMQ is still the cost per job inside the scripts: about 4 to 5 ms of script time per job here against BullMQ's 0.5 to 0.8 ms. This change does not touch it.
- A quiet-host repeat, plus steady and delayed workloads, were not run.
