# Second storage class for the v0.1.5 baselines: memory-backed storage

25 November 2026 · Runwick v0.1.5 behaviour (`src/` differs from the tag only by two job-statistics read fixes, see the [Redis](queue-baseline-v0.1.5-redis.md) and [SQLite](queue-baseline-v0.1.5-sqlite.md) reports) · part of [Stage 1](../plans/queue-throughput-stage-1-plan.md). Companions: [Redis](queue-baseline-v0.1.5-redis.md), [PostgreSQL](queue-baseline-v0.1.5-postgresql.md), [SQLite](queue-baseline-v0.1.5-sqlite.md).

## Why this run exists

The throughput plan asks for a second host or disk class before any public claim. There is no second host here, so all three stores were run again with their data on memory (`tmpfs`), where a commit never waits for a disk. The libraries, tests, rounds and settings are the same as the baselines, with 3 rounds instead of 5. This answers one question: how much of each gap is the disk, and how much is the work each library does?

**Short answer: the disk is not what limits Runwick.** Moving to memory leaves its single-caller rates almost unchanged (PostgreSQL 75 to 78 jobs/s, SQLite 163 to 215, Redis 166 to 221), and its CPU per job is unchanged too. The gaps to the fastest libraries stay about as large as before.

## Results

Jobs per second, median of 3 rounds, min to max in brackets. Same machine as the slow-disk PostgreSQL and SQLite baselines (AMD EPYC, 8 cores), alone, with other services idle (CPU about 90% idle during the runs). Data directories on `tmpfs`: PostgreSQL `/var/lib/postgresql`, Redis `/data` (AOF `always`), SQLite and Solid Queue files in `/dev/shm`. Redis and PostgreSQL still run in Docker with a published port, so every call crosses Docker's port forwarding.

### Redis

| Test | Floor | Runwick | Runwick tuned | Wolverine | BullMQ .NET | BullMQ Node |
|---|---:|---:|---:|---:|---:|---:|
| Add one, 1 caller | 638 | 221 (195–260) | 250 (165–252) | 356 (350–412) | 377 (334–716) | 527 (408–1,279) |
| Add one, 16 callers | 4,024 | 1,073 (854–1,138) | 1,032 (792–1,077) | 3,412 (2,997–3,670) | 2,291 (1,973–2,810) | 3,092 (2,128–3,808) |
| Add one, 64 callers | | 1,651 (1,466–1,654) | 1,224 (1,150–1,432) | 5,085 (2,864–5,437) | 4,802 (4,683–5,253) | 4,701 (4,408–5,182) |
| Bulk, native chunk | | 2,858 (2,447–2,939) | 2,587 (2,023–2,841) | no bulk call | 6,885 (5,339–7,434) | 6,273 (5,510–7,305) |
| Bulk, 100 per call | | 2,340 (2,273–3,134) | 2,824 (2,755–2,958) | no bulk call | 5,013 (3,271–5,625) | 4,051 (3,919–5,419) |
| Drain, 16 slots | | 664 (629–798) | 1,549 (1,433–1,625) | 2,290 (1,645–2,675) | 1,924 (1,377–2,884) | 1,831 (1,539–2,508) |
| Drain, 64 slots | | 1,386 (878–1,415) | 1,882 (1,690–2,075) | 3,630 (2,177–4,050) | 3,900 (3,140–4,500) | 3,713 (3,706–4,098) |
| Steady 500/s | | 478 (357–483) | 468 (456–472) | 471 (418–475) | 449 (384–464) | 428 (366–476) |

All 141 Redis runs reconciled with the store, including every Runwick round: the statistics fix removed the 1 to 3 job shortfall.

### PostgreSQL

| Test | Floor | Runwick | River | pg-boss | Wolverine | BullMQ .NET |
|---|---:|---:|---:|---:|---:|---:|
| Add one, 1 caller | 514 | 78 (72–82) | 193 (192–237) | 236 (205–297) | 111 (106–136) | 259 (225–386) |
| Add one, 16 callers | 5,430 | 567 (552–620) | 1,691 (1,660–2,420) | 1,788 (1,102–1,811) | 870 (658–884) | 1,435 (1,243–1,809) |
| Add one, 64 callers | | 669 (631–703) | 1,514 (1,209–1,967) | 2,337 (1,911–2,678) | 1,273 (1,219–2,420) | 1,292 (1,024–1,649) |
| Bulk, native chunk | | 1,014 (804–1,153) | 15,853 (11,958–19,794) | 13,434 (13,414–14,021) | no bulk call | 7,056 (5,487–9,995) |
| Bulk, 100 per call | | 1,113 (1,099–1,167) | 6,840 (6,609–7,597) | 8,084 (7,769–8,238) | no bulk call | 4,306 (4,188–5,254) |
| Drain, 16 slots | | 328 (292–363) | 1,903 (1,791–2,307) | 1,186 (1,183–1,195) | 69 (68–70) | 658 (583–738) |
| Drain, 64 slots | | 844 (564–954) | 4,277 (4,111–4,736) | 2,464 (2,414–2,469) | 70 (68–71) | 707 (663–725) |
| Steady 500/s | | 263 (232–276) | 439 (352–452) | 439 (402–464) | 68 (all 3 rounds incomplete) | 142 (136–146) |

### SQLite

| Test | Floor (FULL) | Runwick | Wolverine | Solid Queue |
|---|---:|---:|---:|---:|
| Add one, 1 caller | 21,707 (12,933–27,657) | 215 (159–240) | 603 (416–874) | 47 (43–50) |
| Add one, 16 callers | 12,712 (8,197–18,455) | 644 (526–659) | 829 (752–896) | crashed, 3 of 3 rounds |
| Add one, 64 callers | | 607 (489–710) | 769 (618–792) | crashed, 3 of 3 rounds |
| Bulk, native chunk | | 1,625 (1,624–1,950) | no bulk call | 2,420 (2,323–2,501) |
| Bulk, 100 per call | | 1,726 (1,669–1,818) | no bulk call | 1,799 (1,702–1,847) |
| Drain, 16 slots | | 350 (332–359) | 732 (609–742) | 45 (45–47) |
| Drain, 64 slots | | 384 (336–445) | 684 (603–760) | 54 (48–56) |
| Steady 500/s | | 241 (241–271) | 322 (311–360) | 18 (13–22) |

## What the fast disk changes, and what it does not

- **Single-caller adds are not disk-bound for Runwick.** A bare `FULL` write on SQLite runs at 21,707/s here, because nothing waits for a disk, and Runwick still manages only 215/s. It spends about 4.7 ms per add on its own work, against about 0.05 ms for the bare write. PostgreSQL is the same: 78/s with CPU of about 14 ms per job (14 ms on the slow disk too), against River's 1.5 ms. The "ceiling" argument in the [SQLite report](queue-baseline-v0.1.5-sqlite.md) (that the one-caller target is above the bare `FULL` write on that slow disk) is true of that disk only; on memory it does not hold, and the one-caller gap to Wolverine (215 against 603, 2.8 times) is genuinely Runwick's own cost.
- **Docker adds a floor on Redis and PostgreSQL.** The bare write costs about 1.5 to 1.9 ms per call here even on memory (Redis 638/s, PostgreSQL 514/s at one caller), which is Docker's port forwarding plus the client round trip. That is why every library's one-caller rate is low in those two tables, and why 16 and 64 callers matter more than one.
- **The gaps that mattered still matter.** PostgreSQL bulk at 100 per call: 1,113 against 6,840 to 8,084 (6 to 7 times). PostgreSQL drain at 64 slots: 844 against 4,277 (5 times). Redis single adds at 64 callers: 1,651 against 4,700 to 5,085 (about 3 times). Redis drain at 64 slots: tuned 1,882 against 3,630 to 3,900 (about half).
- **Two gaps got smaller.** SQLite bulk at 100 per call is now level with Solid Queue (1,726 against 1,799, 0.96 times); at Solid Queue's own 1,000 per call it is 1,625 against 2,420. Redis drain at 16 slots with the tuned settings reaches 1,549 against 1,831 to 2,290 (68 to 85%).
- **Runwick keeps up at 500 jobs per second only on Redis.** On Redis it finishes 478/s against 428 to 471 for the others. On PostgreSQL it finishes 263 against 439 for River and pg-boss, and on SQLite 241 against 322 for Wolverine.
- **Nothing here changes the explanations.** Wolverine PostgreSQL's drain is 69 to 70/s (its 250 ms listener wait), and Solid Queue still crashes above one caller (its 10 s write lock wait). Solid Queue's SQLite settings are still `NORMAL`; the "not equal durability" caveat holds.

## Per-job cost on the fast disk

Median CPU milliseconds per job, application plus store, timed phase only:

| Test | Runwick | Peer |
|---|---:|---|
| PostgreSQL, add one, 1 caller | 14.4 | River 1.5, pg-boss 3.6 |
| PostgreSQL, drain 16 slots | 6.2 | River 0.7, pg-boss 0.8 |
| Redis, add one, 1 caller | 4.8 | Wolverine 2.8, BullMQ Node 1.3 |
| Redis, drain 16 slots | 2.4 | Wolverine 0.8, BullMQ Node 0.7 |
| SQLite, add one, 1 caller | 6.2 | Wolverine 2.6 |
| SQLite, drain 16 slots | 4.4 | Wolverine 1.4 |

The numbers match the slow-disk baselines closely, so the extra cost is CPU work per job, not waiting. That is the strongest sign so far that Stage 2's first experiment (how much of a dispatch is Runwick's preparation and how much is the store call) is the right place to start.

## Caveats

- Three rounds, one machine, one day. Some ranges are wide (Redis BullMQ Node one-caller add 408 to 1,279). Do not read small differences.
- Redis and PostgreSQL ran in Docker on loopback; the port forwarding costs about 1.5 ms per call and applies to every library equally. SQLite had no such layer.
- Wolverine PostgreSQL's three steady-500 rounds are incomplete (4,700 to 6,100 jobs unfinished after the grace period), as on the slow disk. Solid Queue's 16 and 64-caller rows crashed in every round.
- The Redis and PostgreSQL results are with the Runwick statistics fixes in place; the throughput code paths are unchanged from v0.1.5.
- Raw results and logs: [`evidence/baseline-v0.1.5/tmpfs-sqlite/`](evidence/baseline-v0.1.5/tmpfs-sqlite/), [`tmpfs-redis/`](evidence/baseline-v0.1.5/tmpfs-redis/), [`tmpfs-postgres/`](evidence/baseline-v0.1.5/tmpfs-postgres/).

## Reproduce

```bash
export TMPDIR=/dev/shm/bench && mkdir -p $TMPDIR
python3 eng/bench-compare/ci/run.py --systems writefloor-pg,runwick-pg,river,pgboss,wolverine-pg,bullmq-dotnet-pg \
  --workload normal --rounds 3 --fresh-stores --store-tmpfs --out results
```

For SQLite leave out `--fresh-stores` and `--store-tmpfs` and keep `TMPDIR` on the memory disk; Solid Queue also needs `BENCH_SQLITE=/dev/shm/bench/solid.sqlite3` with its schema loaded.
