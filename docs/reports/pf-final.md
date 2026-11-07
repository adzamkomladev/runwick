# Runwick speed work: final results, before and after

Date: 2026-11-07. Code tested: `799766d` (main). Every speed number below was taken on a shared, busy 8-CPU test machine (load average 3 to 16 during the runs). It is **not a reference machine**. Read the numbers as "clearly faster" or "still short", not as exact figures. Run-to-run swings of 30 to 50 percent were normal here.

## The short version

- Runwick is much faster than before: roughly 4 to 25 times, depending on the test.
- It still does **not** reach the goal of 100 jobs a second on this machine. Two of the three speed tests below fell behind and built up a backlog.
- Nothing was lost or delivered twice in any run, and no guarantee was weakened.
- Against River and pg-boss on the same database, Runwick is ahead of pg-boss and behind River in this setup. The comparison is rough (see the caveats).
- **Update, same day:** a later change (set-based claim, `0243c17`) roughly doubled Runwick's drain speed. Runwick now matches River's default settings at 16 workers and passes it at 64. See "Update: set-based claim" below.

## What changed

| Step | What it does | Effect |
|---|---|---|
| Fewer trips | Independent statements go together; whole operations became single database functions | Trips to the database per job: **64, then 21, then 8** |
| Claim in groups | A worker claims and starts up to 32 jobs in one call | Removes the "one claim at a time" ceiling |
| Finish in groups | A worker reports up to 32 finished jobs in one call (each still has its own receipt) | Finishing no longer needs one trip per job |
| Renew in groups | One call renews all held leases | Fewer writes just to say "still working" |
| Wake-ups | The database tells workers when work arrives | Idle workers send less than a third as many queries |

## The tests

All three: this busy machine, 1 worker with 16 slots unless stated.

| Test | Question | Before | Now |
|---|---|---|---|
| G-1 P-02 | Highest steady rate with instant jobs, without falling behind | about 2 jobs/s | **50 jobs/s** (flat backlog in all three confirmation runs) |
| G-1 P-01 | 100 jobs/s arriving, each job takes 0.1 s | 7 to 14 finished/s | **37, 48 and 62 finished/s** in three repetitions. Backlog grew every time. |
| G-2 P-09 | Outbox relay with 100 messages/s committed | 5.65 delivered/s | **26.3 delivered/s**. Test failed: the backlog reached about 67,000 and did not drain within 60 minutes. |

Details:

- P-02: a few offers were refused by the producer's own local limit (455 to 1,747 per run). That limit is the producer's back-pressure, not the database.
- P-01: about half the offers were refused by that same local limit, because the worker was too slow. The first repetition ran at the lowest rate and the third at the highest. I did not find out why.
- P-09: at the default 4 messages/s it passes (all 4,530 delivered, nothing left). At 100/s the relay (4 slots) tops out near 26/s. The 91 injected lost marks were all redelivered to the same job, with no duplicates.
- Database writes per job: about 12 to 14 KB of write-ahead log, similar in every run.

## The other G-2 workloads (all passed, zero correctness violations)

| Workload | Finishes per second |
|---|---|
| G1-M1, G1-M3 | passed (one run each; rates not compared) |
| P-04 | 42 to 51 with instant jobs; 22.8 with 0.1 s jobs |
| P-05 | 85, 205, 254 at the larger settings; 42 to 46 at the smaller ones |
| P-06r | 47.7, 38.8 and 21.3 over three runs |
| P-06s | 15.5 |

The G-2 case table lists 130 cases as "unimplemented". I did not rerun the test suites inside the G-2 script on purpose, so the table only holds workload evidence. It does not mean those cases broke.

## Tests

On the final code: Core 388 of 388, Outbox 36 of 36, Postgres 317 of 317. No failures in that run.
Tests that fail now and then under load, from earlier runs: CAP-10 (about one run in two or three, before and after these changes), WRK-08 and the store-outage test (tied to how long Docker takes to restart), TIME-04, TIME-09, TXC-05 and MODE-02 (each seen once or twice). Known gap: no test stops admission while batching is on.

## Runwick against River and pg-boss

Same PostgreSQL, no-op jobs, 1 KiB payload, 20,000 jobs per drain run, 120 s per steady run. The machine was busy (load 10 to 17).

| Run | Jobs/s | Notes |
|---|---|---|
| River, default settings, 16 workers | 149 | |
| River, fetch cooldown 1 ms, 16 workers | 823 | |
| River, 100/s arriving | 99.8 | kept up; median 233 ms |
| pg-boss, 1 per fetch, 16 workers | 31 | |
| pg-boss, batch of 16, 1 worker | 543 | |
| pg-boss, 100/s arriving | 31.6 | fell far behind; median 59 s |
| Runwick, 16 workers | 81 | |
| Runwick, 64 workers | 136 | |
| Runwick, 100/s arriving | 50.1 | did not keep up; median 66 ms for the ones that finished |

How to read it:

- Runwick beats pg-boss's default way of working and loses to pg-boss when pg-boss uses batches.
- Runwick is behind River, both at default and tuned, even with more workers.
- A later trace showed the gap was mostly how Runwick claimed jobs (one database function call per job), not its safety rules. River also waits for each change to reach disk.
- Caveats: Runwick's default is 4 workers, so the 16 and 64 worker rows are not its defaults. The pg-boss batch row uses 1 worker. The numbers come from one run each, on a noisy host.
- The first attempt at this comparison failed because the `runwick-pg` container was stopped. It was restarted and the comparison rerun in full.

## Update: set-based claim (2026-11-07, `0243c17`)

A trace of the drain run showed that claiming jobs, not finishing them, was what held Runwick back. Workers sat idle waiting for claims, while the queue of jobs waiting to be finished was nearly empty. The fix: ordinary jobs (no scope, no rate limit, no cancel request) are now claimed and started with one SQL statement for the whole group, instead of one function call per job. Jobs with special rules still take the old path. Schema revision 12.

Two other ideas were tried and not kept:

- **Several claims in flight at once:** it helped a little on its own, but on top of the set-based claim it was slower, and one shutdown test failed once.
- **Changing the claim default:** that idea was dropped together with the one above.

**Drain test, 20,000 jobs, two rounds each (load average 12 to 26):**

| Build | 16 workers | 64 workers |
|---|---|---|
| Before | 86 to 89 | 118 to 119 |
| Set-based claim | 111 to 120 | 209 to 270 |

**Comparison rerun on `0243c17`** (all three systems back to back; load average 4 to 10):

| Run | Before | Now | Notes |
|---|---|---|---|
| River, default, 16 workers | 149 | 142 | |
| River, fetch cooldown 1 ms, 16 workers | 823 | 956 | |
| River, 100/s arriving | 99.8 | 99.4 | kept up; median 213 ms |
| pg-boss, 1 per fetch, 16 workers | 31 | 31.6 | |
| pg-boss, batch of 16, 1 worker | 543 | 721 | |
| pg-boss, 100/s arriving | 31.6 | 31.8 | fell far behind; median 62 s |
| **Runwick, 16 workers** | 81 | **143** | all 20,000 done |
| **Runwick, 64 workers** | 136 | **312** | all 20,000 done |
| Runwick, 100/s arriving | 50.1 | 53.5 | see below |

How to read it:

- Runwick now matches River's default settings at 16 workers and is about twice as fast at 64. Tuned River is still about three times faster.
- Runwick is now well ahead of pg-boss's default mode, but still behind pg-boss in batch mode.
- **The 100/s row is not a worker problem.** The test only managed to *add* 6,431 jobs in 120 s (about 54 a second), and every job it added finished, with a median of 53 ms. Adding jobs one at a time is the limit here. The drain runs show it too: adding the 20,000 jobs took Runwick 19 to 22 s, against 1 to 2 s for River.
- Full test suites passed on the set-based code: Core 388 of 388, Postgres 321 of 321.

## What is not done

- 100 jobs a second is not reached (P-01, P-09, and the steady 100/s comparison). For the steady comparison the limit is now adding jobs, not running them.
- The finish path allows only two finishes at once. That default is unchanged. Widening it would likely help P-01 and P-09 and is your call.
- Batching only happens when a worker pool has one queue ready. Pools that serve several queues still claim one job at a time.
- Memory per job went from 331 to 302 KiB; the goal of 40 KiB is not met.
- The speed-up from fewer trips cannot be shown cleanly on this busy machine. A quiet machine would be needed.
- The G-2 reviewer sign-off in `docs/reports/g2.md` is blank and stays yours.
