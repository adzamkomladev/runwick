# SQLite baseline for v0.1.5

25 November 2026 · Runwick v0.1.5 (`src/` identical to the tag; run at `fcda4d5`) · part of [Stage 1](../plans/queue-throughput-stage-1-plan.md) of the [throughput plan](../plans/queue-throughput-engineering-plan.md). Companions: [Redis](queue-baseline-v0.1.5-redis.md), [PostgreSQL](queue-baseline-v0.1.5-postgresql.md).

## In short

Runwick's SQLite store is behind Wolverine everywhere except bulk adds, and the biggest gap is the one-caller single add (163 jobs/s against 723). It is ahead of Solid Queue in most tests, including bulk at equal chunk size, but those comparisons are not like for like on durability: see the third caveat below.

- **Adding one job at a time:** 163/s with one caller (Wolverine 723, Solid Queue 52), 501 with 16 callers (Wolverine 752), 535 with 64 (Wolverine 865). Runwick does improve with more callers, but starts a long way behind.
- **Bulk:** 2,163/s at 100 per call. Solid Queue manages 1,641 at the same size, and 2,537 at its own 1,000 per call.
- **Processing a backlog:** 302/s at 16 slots and 447 at 64. Wolverine does 801 and 752.
- **Steady arrivals:** keeps up at 100/s. At 500/s it finishes only 153/s and refuses about 9,000 of roughly 15,000 offers over the 30-second window (Wolverine finishes 350, with a worse tail).
- **The bare disk is slow here:** one `FULL` write costs about 2,100 µs on this machine (about 485/s), so Runwick's `FULL` numbers are bounded by the disk. Wolverine and Solid Queue use `NORMAL`, whose bare write runs near 18,000/s, so their numbers are not bounded the same way.

## What was run

One local machine (AMD EPYC, 8 cores, 24 GB, ext4 on `/dev/sda1`), alone for the run, with no other benchmark going. Five rounds of every test, libraries one after another in alternating order, plus a traced round for each Runwick test that is kept out of the numbers. Jobs are 1 KiB no-ops. Libraries: Runwick, Wolverine 6.65.1 (SQLite transport) and Solid Queue 1.2.1 on Rails 8.0.5.2 and Ruby 3.2.3 (SQLite 3.54.1). The floor is one `INSERT` of 1 KiB on a connection set to WAL and `synchronous=FULL`; a second floor at `synchronous=NORMAL` was measured afterwards (five runs each, 1,500 writes): `FULL` 426–551/s with one caller and 437–520/s with 16; `NORMAL` 14,873–18,465/s with one caller and 2,549–11,396/s with 16.

Raw rounds, logs and manifests: [`evidence/baseline-v0.1.5/sqlite/`](evidence/baseline-v0.1.5/sqlite/). The first pass of some cells was superseded; see [Re-runs](#re-runs).

## Results

Jobs per second, median of 5 rounds, min to max in brackets. `F4` means four of the five rounds crashed.

| Test | Floor (bare insert) | Runwick | Wolverine | Solid Queue |
|---|---:|---:|---:|---:|
| Add one, 1 caller | 466 (287–569) | 163 (106–182) | 723 (491–998) | 52 (49–54) |
| Add one, 16 callers | 466 (336–581) | 501 (394–627) | 752 (733–871) | 42 (one round; F4) |
| Add one, 64 callers | | 535 (478–593) | 865 (654–965) | all 5 rounds crashed |
| Bulk, native chunk | | 2,003 (1,944–2,238) | no bulk call | 2,537 (1,987–2,629) |
| Bulk, 100 per call | | 2,163 (1,631–2,297) | no bulk call | 1,641 (1,550–1,820) |
| Drain, 16 slots | | 302 (187–367) | 801 (655–881) | 54 (50–65) |
| Drain, 64 slots | | 447 (301–559) | 752 (673–876) | 68 (60–73) |
| Steady 100/s | | 100 (97–100) | 87 (84–89) | 25 (23–27) |
| Steady 500/s | | 153 (117–200) | 350 (338–376) | 28 (24–30) |

"Native chunk" is each library's own bulk size: 100 for Runwick, 1,000 for Solid Queue. Wolverine's drain rate from first claim is higher than from worker start (912 against 801 at 16 slots) because of its startup. Runwick's two clocks are equal. Steady figures are jobs finished per second including catch-up after the offers stop. All 45 Runwick rounds and all 35 Wolverine rounds (it has no bulk call) reconciled with the database: nothing missing, failed or left unfinished.

### Cost per job, timed phase only (median)

| Test | Library | CPU ms/job | p99 ms |
|---|---|---:|---:|
| Add one, 1 caller | Runwick | 5.4 | 24 |
| | Wolverine | 1.8 | 15 |
| | Solid Queue | 18.9 | 46 |
| Add one, 16 callers | Runwick | 2.3 | 129 |
| | Wolverine | 1.5 | 132 |
| | Solid Queue | 25.1 | 4,276 |
| Bulk, 100 per call | Runwick | 0.54 | |
| | Solid Queue | 0.57 | |
| Drain, 16 slots | Runwick | 4.0 | |
| | Wolverine | 1.2 | |
| | Solid Queue | 51.0 | |

No bytes-on-the-wire figure is shown, because there is no server in the middle. CPU for Solid Queue includes starting Ruby worker processes, which dominates its drain figure.

## Against the targets

Target = 90% of the fastest other library in the same column on this host. Sizing figures only.

| Test | Runwick now | 90% target (leader) | Needed |
|---|---:|---:|---:|
| Add one, 1 caller | 163 | 651 (Wolverine) | 4.0× |
| Add one, 16 callers | 501 | 677 (Wolverine) | 1.35× |
| Add one, 64 callers | 535 | 779 (Wolverine) | 1.5× |
| Bulk, native | 2,003 | 2,283 (Solid Queue) | 1.14× |
| Bulk, 100 per call | 2,163 | 1,477 (Solid Queue) | already ahead (1.3×) |
| Drain, 16 slots | 302 | 721 (Wolverine) | 2.4× |
| Drain, 64 slots | 447 | 677 (Wolverine) | 1.5× |

The bulk gap at native chunk sizes is small and goes away at equal size, which matches the plan's reading that Solid Queue's lead comes from its larger chunk and weaker durability, not a faster write path.

## Caveats that change how to read this

1. **Slow disk.** At one caller Runwick's wall time per add is about 6,100 µs, against a bare `FULL` write of about 2,100 µs. That leaves a lot unexplained even on this slow disk, which is why the one-caller gap is the most useful number here.
2. **Wolverine does not use `FULL`, and the single-caller target is above the `FULL` ceiling on this disk.** Wolverine's one-caller rate (723/s) is faster than a bare insert at `FULL` (about 485/s, five runs 426 to 551). Its SQLite library's connection defaults (Weasel 9.50.0, read from the package, not observed on the harness's connections) are WAL, `synchronous = NORMAL`, busy timeout 5,000 ms. At `NORMAL` a bare insert runs at about 18,000/s with one caller, because the commit does not wait for the disk. So Wolverine's commits do not wait for the disk either, and Runwick's `FULL` commits do. At equal durability the Runwick one-caller target of 651/s is above the bare `FULL` ceiling here, and 163/s is about a third of that ceiling. At 16 callers the bare `FULL` write is about 490/s, close to Runwick's 501.
3. **Solid Queue runs `synchronous=NORMAL`** (recorded: WAL, `synchronous=1`, auto-checkpoint 1000). Runwick requires `FULL`. Solid Queue's bulk and drain figures are therefore on weaker durability than Runwick's, and its rates here are not a fair measure of what `FULL` costs.
4. **Solid Queue crashes under many producers.** With 16 callers, 4 of 5 rounds failed, and with 64 callers all 5, after its 10-second SQLite lock wait ran out. The one 16-caller round that finished did 42/s. Its steady numbers are low too. These are real results for the configuration used (Rails defaults plus a 10 s timeout), not tuned.
5. **Different machine and day from the Redis baseline.** Do not compare across the three baseline reports.

## How noisy these numbers are

On this machine most cells vary by more than 15% between the lowest and highest of five rounds (PostgreSQL: 36 of 45 cells; SQLite: 17 of 26). The disk is the reason: even the bare write ranges widely (PostgreSQL one-caller floor 144 to 393/s). I did not re-run these cells, because more rounds on the same disk would tighten the medians but not the spread. The gaps that drive the conclusions (2 to 14 times for PostgreSQL, 2 to 4 times for most SQLite rows) are much larger than the spread. The small SQLite gaps (bulk at native chunk size, 1.14 times, and 16 or 64 callers, 1.35 to 1.5 times) are inside it and should be re-measured on a quieter disk before anyone acts on them. The Redis run on CI had no cell above 15%.

## Re-runs

- The first pass of Runwick's drains (all 5 rounds) and three of its steady-500 rounds came back "no store counts": the store-count check used Runwick's management statistics, whose read has a time budget that 20,000-job runs on SQLite exceeded. The harness now counts straight from the database file for SQLite, and those cells were re-run alone, one round at a time. Their first-pass results are kept in [`rerun-superseded/sqlite/`](evidence/baseline-v0.1.5/rerun-superseded/sqlite/); throughput was the same within noise.
- The management-statistics read missing its budget on a 20,000-job SQLite store is itself worth a look, separately from this baseline.

## Reproduce

```bash
python3 eng/bench-compare/ci/run.py --systems writefloor-sqlite,runwick-sqlite,wolverine-sqlite,solidqueue \
  --workload normal --rounds 5 --trace-round --out results
```

Solid Queue needs Ruby 3.2 or later and `bash eng/bench-compare/solidqueue/setup.sh` first.
