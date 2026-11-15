# Scheduler work area S: round-two follow-up (2026-11-15)

Everything here ran on the shared host while the SP-029 qualification and four other agents were also running. The host load average was 17 to 50 on eight cores. Treat every timing as informational. The correctness checks are not affected by load: every run acknowledged all 3,000 expected occurrences, with no duplicate instants, no orphan occurrences, no shared jobs and no lease losses.

## What changed

1. **Group sizes 2 to 8 are now an option** (`SchedulerOptions.MaterializationGroupSize`, default 4, PostgreSQL migration 0023, additive). This is the isolated eight-plan candidate from the earlier report, landed with its tests: Core scheduling 181, PostgreSQL migrations 72 and the schedule contracts 73 (SQLite, PostgreSQL and Redis) all passed. Migration 0023 takes the next free number, so it will need renumbering if another change lands a migration first.
2. **The grouped loop is pipelined.** It reads the next due page while the current page commits, keeps groups in flight across the page boundary, and prepares the next group while both slots are busy. Before this, each page of 32 schedules drained both groups, then waited for a read, then waited for the first new group to fill. Two new Core tests cover it: the second page is read before any group commits, and shutdown leaves no group running after a full page. Spec 13 has a dated note.
3. **The P-06s workload gained `--scheduler-replicas N`** (equal aggregate allowance: 100/N jobs/s and burst 32/N per replica) and `--jobs-per-second` / `--burst` for the scheduler process.

## Pipelining, paired short runs (one round)

Same 1,000 schedules, 100 jobs/s allowance, two groups in flight, two minutes of warm-up, three of measurement, a fresh PostgreSQL 18.6 container per run. "Base" is commit 295acb9 and "pipelined" is a85849f. Post-commit acknowledgment lag:

| Arm | p50 | p95 | p99 | Mean due-read wait | Mean function per group |
|---|---:|---:|---:|---:|---:|
| Four plans, no pipelining | 9.55 s | 17.40 s | 18.61 s | 36.7 ms | 90.8 ms |
| Eight plans, no pipelining | 8.96 s | 20.44 s | 22.18 s | 48.1 ms | 146.4 ms |
| Eight plans, pipelined | 8.89 s | 20.28 s | 22.42 s | 6.6 ms | 154.7 ms |

Pipelining did what it was meant to do: the loop now almost never waits on a due read (48 ms down to 6.6 ms per page). It did not move p99 here. At this host load the database function took 146 to 155 ms per group of eight, against 95 ms on the quiet host, so two groups in flight give about 70 plans/s, below the 100/s allowance. The run is limited by the database, not by the loop. One pair is not enough to separate a small gain from noise, and the later rounds were cancelled to free the shared benchmark lock. The earlier quiet-host eight-plan runs (p99 12.47 and 13.86 s) remain the closest evidence for the 13 s gate, and the pipelined loop needs a quiet-host repeat before it can be called better.

Why the 13 s gate is still open: per plan, the database function costs 12 to 16 ms at every group size, so grouping saves round trips and commits but not the per-plan work (job and payload insert, two events, the occurrence row, the schedule update, the receipt). That work is the next lever, and any change to it needs its own replay and rollback proofs. I did not touch it.

## S6: replica contention at equal aggregate allowance

One run each, eight-plan pipelined groups, 1,000 schedules, aggregate 100 jobs/s split evenly:

| Schedulers | Plans sent for 6,000 occurrences | Revision conflicts | p50 | p99 |
|---:|---:|---:|---:|---:|
| 1 | 6,000 | 0 | 8.65 s | 16.86 s |
| 2 | 8,405 | 1,464 | 11.19 s | 21.63 s |
| 4 | 15,650 | 7,240 | 20.08 s | 39.32 s |

Contention is material. Every replica reads the same ordered head of the due index, so they plan the same schedules, and the loser of each race wastes a plan and a lock wait. Two replicas send 40% more plans than needed and four send 2.6 times as many. Correctness holds (zero duplicates), but adding replicas makes lag worse, not better, at a fixed aggregate allowance. Revision checks remain the safety mechanism. Conflict counts do not depend on host load, so this conclusion is sturdier than the lag figures.

I did not build an ownership protocol. The plan asks for crash recovery, rebalance and abandoned-claim tests for that, and it is a design of its own. The cheapest direction to evaluate is a short claim written through the existing revision check (advance a schedule's scan time by a few seconds before planning it), so a second replica skips it; the cost is one more write per schedule.

## Decisions on the other S items

- **S2 (overlap independent plans):** superseded by the grouped S3 path, which already keeps two groups in flight. Four pending commands queue behind the two-slot PostgreSQL background lane, so the lane is the cap.
- **S4 (durable due hints):** landed earlier; unchanged.
- **S5 (cache compiled recurrences):** not justified. Evaluation costs 0.064 ms per plan on average (0.4 ms at p99), against about 12 ms of database work per plan.

## What is still open

- A quiet-host P-06s repeat of the four- against eight-plan, pipelined and not, including the 15-minute steady window and the 1,000-occurrence catch-up.
- The per-plan database cost (see above).
- A replica ownership design, if running more than one scheduler matters for P-06.

Raw runs: `artifacts/sched-s/arms/` and `artifacts/sched-s/replicas/` in the worktree (git ignores them), with `run-arms.sh`, `run-replicas.sh` and `summarize.py` beside them.
