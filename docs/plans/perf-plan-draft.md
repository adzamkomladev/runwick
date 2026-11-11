# Making Runwick faster: the plan

*Round one closed. Status updated 2026-11-11. Remaining performance work is tracked in [the round-two plan](perf-plan-2.md).*

## The problem in one paragraph

Runwick is slow because it talks to the database too often. Taking one job from "dispatched" to "finished" used to take about 64 separate trips to PostgreSQL and back. Each trip costs about 3 ms on this machine, and up to 7 ms when the machine is busy. So one worker could handle only about 5 jobs a second, while the goal is 100. The database itself isn't the bottleneck: connection pools, locks and disk all measured close to zero waiting. The time goes on the trips.

## How other job queues get their speed

We looked at BullMQ, Laravel Horizon, the Laravel database queue, Hangfire, River, pg-boss, JobRunr, Wolverine and GoodJob. They do five things:

1. **One trip per step.** A whole state change is sent as a single script or a single SQL statement.
2. **Many jobs per trip.** They claim, finish or renew jobs in groups rather than one at a time.
3. **Being told when work arrives**, instead of asking the database over and over.
4. **Preparing statements once** and reusing them.
5. **Not writing to the database just to say "I'm still working on this".**

Some of their speed also comes from guarantees Runwick deliberately keeps and they don't. Runwick refuses a stale worker's late write. It keeps a record that makes every command safe to retry. It waits until each change is safely on disk. We are not giving any of those up.

## Ground rules

- **No guarantee gets weaker.** Each change says which part of the spec it touches and why the behaviour stays the same.
- **Measure before and after**, on the same machine, taking turns between the old and new versions. Note how busy the machine was.
- **Count trips first.** The number of trips per operation is exact and the same every run, so a test checks it. Timings on this machine are noisy, so a timing only counts when it's taken on a quiet machine.
- **Numbers from this machine are labelled "not reference".** You chose to use this host rather than get a dedicated one.

## The steps, and where each one stands

| # | What | Status |
|---|---|---|
| 1 | **Measure.** A test fails the build if any operation starts making more trips. A script (`eng/run-perf.sh`) measures speed, CPU and memory. | Done |
| 2 | **Send independent statements together.** The opening checks every operation makes are sent as one trip instead of several. | Done |
| 3 | **Prepare statements once.** Also keep a couple of connections open so a lease renewal never waits for a new connection. | Done |
| 4 | **Group statements inside each operation**, wherever the later statements don't depend on the earlier answers. | Done. A job now takes **21 trips instead of 64**. |
| 5 | **Move the hot-path operations into database functions.** | Done. The measured lifecycle fell to about eight trips. [Round-one report](../reports/pf-final.md). |
| 6a | **Renew many leases in one call.** A worker renews all the jobs it's holding in one call instead of one call each. | Done |
| 6b | **Claim several jobs in one call** and start them straight away. | Done for a single eligible lane. [Set-based claim](../reports/pf-final.md#update-set-based-claim-2026-11-07-0243c17). |
| 7 | **Wake workers when work arrives.** When a job is added, the database tells waiting workers, instead of workers checking every second. Regular checking stays as the backup. | Done. Idle workers now send less than a third as many queries. |
| 8 | **Cut CPU and memory per job.** | Round-one reductions shipped. The 40 KiB target carries forward to round two. [Performance qualification](../reports/sp-029.md). |
| 9 | **Re-run the G-1 and G-2 speed tests** to see where we stand. | Non-reference workloads recorded. [Round-one report](../reports/pf-final.md), [performance qualification](../reports/sp-029.md). |
| — | **Compare with River and pg-boss** on the same database, for information only. | Done. [PostgreSQL comparison](../reports/perf-investigation.md). |

The round-two plan records the later throughput measurements and the release qualification still owed on a quiet host.

## Things we chose not to do

| Idea | Why not |
|---|---|
| Bigger connection pools | Nothing is waiting for a connection now, so more connections wouldn't help. |
| Skip waiting for the disk on commit | A crash could then lose a change the client was told had succeeded. |
| Let many operations share one connection at once (Npgsql "multiplexing") | Runwick needs each transaction to have its own connection. |
| Use database locks as leases, as GoodJob does | If a connection drops, a second worker could start the same job while the first is still running it. |
| Flush to disk once a second, as Redis does | That's an acceptable trade-off for a future Redis driver, not for PostgreSQL. |

## Decisions that are yours

The full list of ten questions is in section 10 of the step 5 proposal. The main ones:

1. **Try step 5 on a small scale first?** The suggestion is to convert two operations, claiming a job and renewing a lease, then measure. Go ahead only if claiming gets at least 30% faster.
2. **Is a quiet machine available** to measure that trial on? This one's numbers swing too much to decide on.
3. **Step 5 before step 6b?** If step 5 goes ahead, it rewrites the claim code, so writing 6b first would mean writing it twice.
4. **Upgrades:** should a release accept both the old and the new database layout during an upgrade, or should you stop the workers and migrate?
5. **The idle check interval:** with wake-ups in place, workers could check less often when idle. That changes a test limit and a default, so it's your decision.

## G-2

The G-2 report is ready (`docs/reports/g2.md`). Every case passed or doesn't apply, and none failed. The reviewer sign-off is left blank for you to fill in.
