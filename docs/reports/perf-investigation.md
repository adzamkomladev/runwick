# Runwick on PostgreSQL: where the speed goes, and what to change next

Date: 2026-11-08. Code tested: main at `f5b5c99`, plus one prototype (branch `claude/perf-proto-wake`, commit on top of main). Everything here is measured against `runwick-pg` (PostgreSQL 18.6 in Docker, reached through docker-proxy, 8 CPUs, default settings, `synchronous_commit=on`). Each system used its own freshly created database. Jobs do nothing and carry a 1 KiB payload.

## The short version

- **Runwick is slowest of the three at adding jobs, by a lot.** On a quiet machine: 63 adds a second from one caller (River 158, pg-boss 178) and 306 a second from 16 callers (River 1,711, pg-boss 1,316). That is about 2.5 to 3 times slower from one caller and 4 to 6 times slower from 16.
- **Running jobs is closer.** With 16 workers Runwick drains 188 jobs a second, slightly ahead of River's default (157) and well behind pg-boss in batch mode (851). With 64 workers, Runwick reaches 381 and River with its fetch wait cut to 1 ms reaches 3,621, about 10 times more.
- **The quickest win is small.** Runwick sent its "work has arrived" message inside every add transaction. PostgreSQL makes every such transaction queue behind one cluster-wide lock at commit, so concurrent adds took turns. Sending the message after the commit instead raised 16-caller adds from 332 to 451 a second (median of 6 interleaved rounds; the prototype won every round). It did nothing for single-caller adds or for draining, as expected.
- **The big win is already in the code but not used by the normal add call.** Adding 100 jobs in one call (`DispatchMany`) puts 20,000 jobs in about 5 seconds, roughly 3,900 a second, about 13 times the 16-caller single-add rate. Grouping concurrent single adds into such calls inside the producer is the highest-value change.
- Nothing here weakens a guarantee. The ranked list below says, for each change, what it touches.

## How the numbers were taken

- Harness: `eng/bench-compare/` (`run.sh` for the three-way comparison, `ab.sh` for two Runwick builds, `summarize.py` for medians). Versions: River v0.50.0, pg-boss 12.38.0 (pg 8.23.1), both pinned.
- Each test runs all systems back to back, repeated 3 times, with the order rotated each round. The A/B alternates the two builds and flips the order every round.
- Another session was building and testing on the same machine for most of the time. Every run records cores, the load average and the busiest processes before and after, and a run is flagged `*` when the one-minute load average at its start was above 8.
- **Two sets of numbers, and which to trust.** The first full comparison (`baseline`) ran at load 13 to 88 on 8 cores, so all 51 runs are flagged heavy and swings of 2 to 3 times between rounds were common. After the host quieted down (load 2 to 12) I reran the enqueue and drain tests (`quiet`) and the A/B. **The tables below use the quiet runs.** The heavy-load baseline is kept in the appendix because it covers more tests (steady arrival rates), but read it only for ordering, not for values.
- Small flaw in the harness: the "busiest processes" list in each `*.load.json` includes the snapshot script's own `ps` and `python3`. The load averages are right; ignore the process list.
- This is not a reference machine. One Docker container, one host.

## Measured: Runwick against River and pg-boss (quiet host)

Median of 3 rounds, with the lowest to highest in brackets. Load at start was 2.6 to 11 (flagged runs in the right column).

| Test | Runwick | River | pg-boss | Runs started above load 8 |
|---|---|---|---|---|
| Add one job at a time, 1 caller (jobs/s) | **63** (56–64) | 158 (139–163) | 178 (111–183) | Runwick 1 of 3, pg-boss 1 of 3 |
| Add one job at a time, 16 callers (jobs/s) | **306** (264–309) | 1,711 (1,351–1,848) | 1,316 (1,064–1,407) | River 2 of 3, pg-boss 1 of 3 |
| Per add call, 16 callers: median / 99th percentile (ms) | 43 / 220 | 8 / 25 | 10 / 45 | |
| Drain 20,000 jobs, 16 workers (jobs/s) | **188** (170–194) | 157 (155–157) (default settings) | 851 (797–944) (1 worker, batch of 16) | Runwick 1 of 3, River 1 of 3, pg-boss 1 of 3 |
| Drain 20,000 jobs, 64 workers (jobs/s) | **381** (378–400) | 3,621 (2,766–3,761) (fetch wait 1 ms) | not run | none |
| Time to add 20,000 jobs in bulk (s) | 5.1 (8 callers, 100 per call) | 0.8–1.3 | 1.6–1.7 | |

Notes on the table:

- Runwick's drain rows use its shipped batched claim. The 64-worker row is not Runwick's default (default is 4 workers); the River rows use the settings the earlier report used so the figures can be compared with it.
- The add-latency row means: with 16 callers, a typical add call takes Runwick 43 ms and River 8 ms.
- Steady arrival tests (20 and 100 jobs a second) were only run under heavy load. At 20 a second all three finished every job they were given (finished/s median: Runwick 13, River 19.8, pg-boss 17.6, with Runwick between 10 and 20). At 100 a second River finished 93, pg-boss 30 (it falls far behind, median wait 27 s), and Runwick 24 because it could not add jobs fast enough, not because it could not run them. All of these are heavy-load runs.

## Measured: the prototype, wake message after commit

The prototype keeps everything else the same. Direct `Dispatch` and `DispatchMany` tell the database not to send the "work has arrived" message inside the transaction; after the commit, the store sends it once per application and queue, with at most one in flight and any extra adds folded into one more. A job that is not yet due wakes nothing, as before. Polling stays the backstop. It adds schema revision 18.

Interleaved A/B, main against prototype, 6 rounds of the 16-caller add test and 3 of each other test:

| Test | Main | Prototype | Notes |
|---|---|---|---|
| Add, 16 callers (jobs/s) | 332 (244–400) | **451** (361–500) | Prototype won all 6 rounds: 462 vs 288, 361 vs 334, 440 vs 244, 421 vs 364, 500 vs 330, 483 vs 400. 5 of 6 rounds started at load 9 to 13. |
| Add, 16 callers: median / 99th percentile per call (ms) | 40 / 157 | 31 / 110 | |
| Add, 1 caller (jobs/s) | 64 (61–64) | 57 (47–79) | No difference worth reading; ranges overlap |
| Drain, 16 workers (jobs/s) | 182 (180–191) | 178 (171–198) | No difference |

What the lock looked like while 16 callers added jobs (a sample of what each database session was doing every 50 ms):

| | Sessions waiting on the notify lock | Sessions waiting for the disk write to finish (group commit) |
|---|---|---|
| Main | 445 of about 790 samples | 28 plus 10 |
| Prototype | 33 of about 490 samples | about 190, mostly several commits sharing one disk flush |

That is the mechanism: commits no longer queue behind one another, so PostgreSQL can flush several together. The earlier heavy-load A/B (3 rounds, load 25 to 80) was inconclusive and is not used.

Tests run on the prototype: the wake-up tests, dispatch, outbox, staging and migration tests passed (72 and 54). I did not run the full Postgres suite. The pinned schema manifest in `MigrationTests.cs` was bumped from 17 to 18, which is expected for a new revision.

## Where the time goes, per add

All of this is from one add of one job on current main.

- **Trips:** 2 per add. The first sends the settings, the checks and the add function in one batch; the second is the `COMMIT`. Connection opening costs close to nothing (0.005 ms median, the pool is warm).
- **Time on the server, from `pg_stat_user_functions`** (taken under load of about 30, so read the proportions, not the milliseconds): the add function total 13.7 ms of which the core add logic is 9.0 ms, the receipt step 1.4 ms, the final receipt update 1.3 ms, the "was this decided" assertion 1.1 ms and the two gate checks 1.1 ms. About a third of the function time is the receipt and gate machinery that makes the call safe to retry. The rest is the actual add.
- **Rows written per add:** 5 inserts (job, job generation, payload, queue event, command receipt) and 1 update of the receipt row, which is almost always a cheap in-page update. The jobs table has 9 indexes.
- **The commit** is the other half: p50 9.5 ms against 17 ms for the function in the same traced run, under load.
- **Contention:** with 16 callers, more than half the sampled time was spent waiting on the notify lock described above. Nothing else (row locks, buffer locks) came close.
- **Running a job** was covered by the earlier performance reports: 8 trips per job after the PF-05 to PF-08 changes, about 12 to 14 KB of write-ahead log per job.

## How River and pg-boss do the same work

| Step | River 0.50 | pg-boss 12.38 | Runwick (main) |
|---|---|---|---|
| **Add one job** | One `INSERT`, committed on its own. A wake-up message is sent at most once per queue per fetch wait (100 ms default), by a limiter. | One `INSERT` through a small SQL function. No wake-up message by default; workers poll. | One database function call, then a separate `COMMIT` (2 trips). Writes 5 rows and updates a receipt row. Takes shared locks on two single "settings" rows. **Sends a wake-up message inside every transaction.** |
| **Claim jobs** | One `UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED LIMIT n) RETURNING`. Fetches at most once per fetch wait. | Same pattern: `SKIP LOCKED`, a batch per fetch. | One set-based statement for ordinary jobs, up to 32 per call (PF-06). |
| **Finish jobs** | The handler returns at once. Finished jobs wait in memory and are marked complete in one `UPDATE` per batch, every 50 ms once 100 are waiting, otherwise every 250 ms. | One `UPDATE … WHERE id = ANY($ids)` per fetched batch. | Each job's finish is confirmed with its own receipt, up to 32 per call, at most 4 calls at once (PF-06). |
| **Wake-ups** | `LISTEN/NOTIFY`, rate-limited per queue. | Polling (`NOTIFY` is optional in v12). | `LISTEN/NOTIFY`, one notify per add transaction. |
| **Disk** | Waits for every commit to reach disk. | Same. | Same. |

What Runwick does that these two do not, on purpose: a receipt per command so any call can be retried safely, refusal of a stale worker's late write, and a confirmed finish per job. Those cost rows and checks, but the lock finding above shows the add slowdown is not mostly that cost: the wake message was.

Not on the list, and why (all of these appear in the earlier plan too): unlogged tables or turning off the disk wait on commit would lose changes after a crash that a caller was told succeeded; pg-boss and River do not do this either. Letting calls share one connection (Npgsql multiplexing) would break per-transaction connections. Advisory-lock leases fail when a connection drops.

## What to change, ranked

Gains are for the tests above on this kind of machine. "Measured" means I measured it; the others are estimates from the evidence stated, and need a prototype before anyone relies on them.

| # | Change | Expected gain | Risk | Status |
|---|---|---|---|---|
| 1 | **Group concurrent adds inside the producer.** When several callers add one job each at about the same time, send them as one `DispatchMany` transaction, each job still with its own command receipt and outcome. | Large for concurrent adds. Bulk `DispatchMany` ran at about 3,900 jobs a second against 306 for 16 single adds, so a 5 to 10 times gain at 16 callers is plausible, and it removes most commits and wake messages. A lone caller gains nothing, or adds a short wait. | Medium. Needs a small collection window (a few ms) or a size/time trigger, and per-item error and retry semantics must stay as they are. A failed item must not fail its neighbours. Needs the spec's wording on `Dispatch` ordering checked. | Estimate. Bulk rate is measured, grouping is not. |
| 2 | **Send the wake message after commit** (the prototype). | +36% on 16-caller adds, measured. No effect on one caller or draining. It also removes a hidden limiter once change 1 is in, because group commit then helps more. | Low to medium. A lost message only delays pickup to the next poll, which is already the backstop. The prototype re-creates the existing function under the same name; the real change should ship it as a new function version (`rw_dispatch_core_v4`) and a proper revision, with a test that a delayed job wakes nothing and a failed notify does not fail the add. | Measured. Branch `claude/perf-proto-wake`. |
| 3 | **Raise the number of finish calls in flight, and widen what finishes together.** Four finish calls at once, 32 jobs each, is the current cap. River finishes whole batches in one statement, and at 64 workers it is about 10 times faster. | Up to 2 to 4 times on 64-worker drains if the finish lane is the limit; the PF reports say it already was for P-01 and P-09. | Low. It is a default (and a pool size), not a rule. The finish-lane setting was left for the owner in the earlier report. | Estimate. Not measured here. |
| 4 | **Trim the per-add write cost.** Review the 9 indexes on `jobs` and the 5-row footprint; for example, a partial index for the ready lookup, and putting the queue event write behind the receipt only when a reader needs it. | Perhaps 10 to 30% per add and less write-ahead log; unmeasured. | Medium to high. Index changes can slow the claim query or break plans checked by the plan tests; event rows are part of the observable history. Do it last, with the plan tests. | Idea only. |
| 5 | **Let a producer pipeline its two trips.** Send the commit in the same trip as the function. The earlier report built this and found no gain on a busy machine. | Probably small. | Low. | Already tried (PF final). Retest only on a quiet machine. |

Order of work if you want the biggest gain for the least rewriting: 2 first (it is already built and measured, and it makes 1 work better), then 1, then 3.

## What is not done, and what to treat carefully

- No steady-rate test (20 and 100 a second) was repeated on the quiet host; those rows are heavy-load runs.
- The grouping change (1) and the finish-lane change (3) are estimates. The prototype for the wake message is the only change I built and measured.
- The prototype is a proof, not a patch to merge. It keeps the old function name, edits a function by re-creating it, and only the targeted tests ran. The full Postgres suite and the Core suite were not run on it.
- Only one quiet window was used. The 16-caller A/B started above load 8 in 5 of 6 rounds (9 to 13), so values are approximate; the direction (prototype ahead in all 6 pairs) is the part to trust.
- Nothing is merged to main. Both the report and the harness are on `claude/hungry-allen-32efc5`; the prototype is on `claude/perf-proto-wake`.

## Appendix: the heavy-load baseline

Same harness, 3 rounds, every run started above load 8 (13 to 88 on 8 cores). Median, with the range in brackets. Use for ordering only.

| Test | Runwick | River | pg-boss |
|---|---|---|---|
| Add, 1 caller (jobs/s) | 30 (23–44) | 77 (47–82) | 87 (76–106) |
| Add, 16 callers (jobs/s) | 113 (71–143) | 505 (186–876) | 517 (233–943) |
| Drain, 16 workers (jobs/s) | 94 (44–96) | 123 (101–147) | 380 (74–711) |
| Drain, 64 workers (jobs/s) | 213 (122–278) | 2,434 (714–3,570) | not run |
| 20 a second arriving, finished/s | 13 (10–20) | 19.8 (19.7–19.9) | 17.6 (17.4–19.8) |
| 100 a second arriving, finished/s | 24 (14–30) | 93 (61–98) | 30 (29–31) |

Raw files: `artifacts/bench-compare/baseline`, `artifacts/bench-quiet`, `artifacts/ab-quiet-c16`, `artifacts/ab-quiet-rest`, `artifacts/ab-wake` (the inconclusive heavy-load A/B). These are git-ignored and live in the worktree.
