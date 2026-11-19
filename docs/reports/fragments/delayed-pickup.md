## Delayed jobs picked up seconds late (Redis)

### Short answer

The multi-second tail came from the **first delayed jobs of a run**, not from steady running. When a worker had been idle for a while (the benchmark's 12-second warm-up is enough), its idle wait had grown to its 10-second cap. A delayed dispatch sent no wake-up, and the worker's last readiness check had seen nothing scheduled, so it slept through the first due jobs: up to about 4 seconds in our runs, and up to 12 seconds in the worst case. The Redis store now sends a wake-up when a delayed (or retrying) job becomes the earliest waiting job of its queue and priority. The worker wakes, learns the due time from its readiness check, and sleeps until then. In eight paired rounds the multi-second tail went away: the worst round on main had a p99 of 3.8 s, and the worst with the fix was 1.5 s. That 1.5 s round came from slow store calls on a busy host, not from an idle sleep. Median p99 went from 0.69 s to 0.39 s, against 0.35 s for BullMQ.

### How the cause was found

A temporary trace (not committed) logged every idle wait of the pool (back-off step, the next-due hint, how long it slept, and whether a wake-up ended it), every readiness view and every claim, and in the benchmark every job that started more than 1 s after its due time. Fourteen traced rounds of the delayed workload (100 offers/s, 5 s ahead, 16 workers) were run on main's code.

- In every slow round, all the late jobs were among the first offers of the run. Example (round r4): the pool began a 9.5 s idle sleep about 1 s before the first offer. That sleep had no hint, because nothing was scheduled when it started. The first jobs fell due 4 s into the sleep and waited until it ended, so the first ~400 jobs started up to 3.7 s late (p99 3.5 s). Round r6 was the same, with an 8.4 s sleep and up to 3.2 s late.
- Rounds without a tail were lucky in timing: one of the growing idle sleeps happened to end after the first delayed dispatch but before it fell due, the readiness check then named the due time, and the pool slept exactly until then.
- In steady running nothing was missed. Almost every idle wait ended through a wake-up from a finished job, or ran to the next-due hint and claimed at once. No hint was dropped or stale.
- The rare sub-second tails left over were single slow store calls. One `ReserveAndBegin` took 920 ms while the host load was above 10, and the first finishes after start-up also took up to 0.9 s.

So the engine's idle and hint logic worked as designed. The gap was in what the store announced: it sent wake-ups only for jobs that are due now (perf round two F, wake hints), and spec 05's idle-caps entry lists delayed dispatch as "not notified", which costs up to 12 s on a worker idle at the cap.

### The fix

`U.wake` in `src/Runwick.Redis/Lua/prelude.lua` (the hint each write script publishes once its writes are whole) now also publishes the queue name when a written `ready:<queue>:<priority>` member is not due yet but is now the lowest future score in that set. This costs one `ZRANGE … BYSCORE LIMIT 0 1` per written ready set with a future member. Jobs that queue up behind an earlier delayed job publish nothing. In a steady delayed stream that means about two publishes per run (counted in the benchmark's Redis stats), and an empty store gets no extra polling. The engine is unchanged: a wake-up makes the idle pool check readiness, which returns `NextDueAt`, and the pool sleeps until then (05 §7.2, SP-012).

No stored data, reply or receipt changes. Spec: 22 Changes (2026-11-19, delayed pickup) and 05 Changes (2026-11-19, delayed pickup). Public page: `redis.mdx`, "Wake-ups and polling".

PostgreSQL and SQLite are unchanged. Neither sends such a hint nor reports `NextDueAt`, so there a delayed job sent to a long-idle worker is still found within the idle back-off cap (+20 %) plus the hint age. Fixing that would need changes to the PostgreSQL dispatch SQL and its probe, which is a separate piece of work.

### Tests

- Redis: `RedisWorkerPathTests.A_dispatch_publishes_a_wake_when_it_is_due_now_or_the_earliest_delayed_job_of_its_lane`. It replaces the old test, which asserted that a delayed dispatch publishes nothing. It checks four cases: the first delayed job wakes, a later one does not, an earlier one wakes again, and a due-now one wakes. It **fails on main** (no wake for the first delayed job) and passes with the fix.
- Core: `WorkerEngineTests.A_pool_idle_at_the_long_cap_starts_a_delayed_job_at_its_due_time_after_its_wake_hint`. A pool idle at the 10 s cap gets the wake-up and a store that names the due time 3 s ahead. The test checks that the job is reserved within 200 ms of its due time. The engine did not change, so this test passes before and after; it pins the engine half the fix relies on. (A Core test cannot fail before this fix, because the missing piece was the store's wake-up.) The scripted store gained a `NextDueAt` setting.

### Benchmark: delayed 100/s, 5 s ahead, 16 workers

Eight rounds, each with three arms in rotating order, under the shared bench lock, with a fresh Redis 8.2 (AOF always, host disk) per arm. "Fix" is this branch, "main" is 391f520, and BullMQ is 6.7.2. The host was shared with other test suites, so load is recorded. Wait past due is in ms. For Runwick it is `latencyMs − 5000`, which runs from the dispatch call to the finish, so it slightly overstates the wait. For BullMQ it is handler start minus due.

| Round | Load fix / main / BullMQ | Fix p99 | Fix max | Main p99 | Main max | BullMQ p99 | BullMQ max |
|---|---|---|---|---|---|---|---|
| 1 | 10.5 / 8.7 / 9.6 | 359 | 534 | 734 | 895 | 1,357 | 1,625 |
| 2 | 13.5 / 12.2 / 14.1 | 391 | 554 | 750 | 930 | 636 | 797 |
| 3 | 9.2 / 8.0 / 10.4 | 391 | 575 | **3,773** | **3,989** | 255 | 329 |
| 4 | 9.2 / 7.5 / 7.5 | 250 | 341 | **3,235** | **3,498** | 265 | 420 |
| 5 | 6.2 / 8.5 / 6.3 | 727 | 911 | 183 | 318 | 654 | 938 |
| 6 | 5.3 / 5.4 / 6.4 | 1,547 | 1,669 | 498 | 619 | 277 | 383 |
| 7 | 6.8 / 11.0 / 8.4 | 378 | 583 | 652 | 729 | 236 | 345 |
| 8 | 16.6 / 5.7 / 3.9 | 1,105 | 1,288 | 312 | 472 | 414 | 607 |
| **Median** | | **391** | **579** | **693** | **812** | **346** | **514** |

Throughput was the same in all arms (Runwick completions 82 to 86/s over the whole run including drain, every offer settled; BullMQ refused 0 to 150 offers locally). In fix rounds 6 and 8, finish calls were slow (handler-to-finished p99 406 and 226 ms, against about 90 ms in main's rounds 3 and 4). That points to store-call delay on a loaded disk rather than an idle sleep; the traced runs of the fix showed the same pattern (see above). Median wait past due was 36 to 56 ms with the fix and 31 to 77 ms on main.

**Drain check (20,000 jobs, 16 workers, jobs/s, paired rounds).** A drain has no delayed jobs, so the new check never runs. Redis counted the same number of `ZRANGE` calls in both arms (about 3,150 per run) and the same number of `PUBLISH` calls (200 per run).

| Log on | Fix (rounds) | Main (rounds) | Median fix / main |
|---|---|---|---|
| host disk, fsync always (load 5 to 12) | 400, 536, 445, 391, 413, 366 | 506, 518, 278, 415, 447, 425 | 405 / 436 |
| tmpfs, fsync always (load 5 to 8) | 702, 666, 612, 741 | 676, 593, 675, 618 | 684 / 646 |

On the host disk the spread inside each arm (278 to 536) is far wider than the gap between the medians, and on tmpfs the fix comes out ahead. With identical Redis command counts, neither result shows a throughput change.

### What is left

- **PostgreSQL and SQLite:** no `NextDueAt` and no hint for delayed dispatch. A delayed job sent to a long-idle worker can still start up to the back-off cap late: 12 s with a healthy PostgreSQL listener, 2.4 s on SQLite.
- **Other cases with no wake-up on Redis:** jobs released by another process's recovery, a queue resumed by another process, and room freed by rate or concurrency limits in another process are still found by polling (05 idle caps).
- **Sub-second to 1.5 s tails under host load** come from slow store calls (fsync always on a shared disk), not from the idle path. A quiet-host rerun would separate the two more cleanly.
- The idle pool still steps up its back-off from 100 ms while it waits for a known due time, which costs about 6 readiness checks per 5 s wait. That is cheap, so it was left alone.
