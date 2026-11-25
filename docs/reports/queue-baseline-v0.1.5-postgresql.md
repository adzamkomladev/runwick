# PostgreSQL baseline for v0.1.5

25 November 2026 · Runwick v0.1.5 (`src/` identical to the tag; run at `fcda4d5`) · part of [Stage 1](../plans/queue-throughput-stage-1-plan.md) of the [throughput plan](../plans/queue-throughput-engineering-plan.md). Companions: [Redis](queue-baseline-v0.1.5-redis.md), [SQLite](queue-baseline-v0.1.5-sqlite.md).

## In short

PostgreSQL is where Runwick is furthest behind, and the gap is largest in bulk adds and in draining a backlog.

- **Bulk adds:** 1,094 to 1,143 jobs/s. River does 9,846 at 100 per call and 17,502 at 1,000, pg-boss 8,471 and 15,018. That is about 8 to 14 times faster.
- **Draining a backlog:** 380/s at 16 slots and 715 at 64, against River's 1,890 and 5,462 (5 to 8 times) and pg-boss's 1,184 and 2,325.
- **Adding one job at a time:** 75/s with one caller (peers 195 to 229), 508 with 16 callers (peers 628 to 1,629), 672 with 64 (peers 660 to 1,948). At 64 callers Runwick is level with BullMQ .NET, but 2.6 to 2.9 times behind River and pg-boss.
- **Steady arrivals** at 100/s are fine. At 500/s Runwick finishes 295/s, while River and pg-boss finish about 425 to 435, and the latency tail is very long (p99 about 20 s).
- **Work per job is the visible difference.** For each job drained Runwick writes about 7,600 to 8,000 bytes of write-ahead log; River writes about 4,000 and pg-boss about 3,000. Bulk adds cost Runwick 1.06 ms of CPU per job against 0.05 for River.
- **Two of the original anomalies are explained.** Wolverine's PostgreSQL drain is 70 jobs/s in every round, matching its 250 ms listener wait. BullMQ .NET's drains all finished this time, at about 550 to 570/s, once the harness counted failures properly.

## What was run

One local machine (AMD EPYC, 8 cores, 24 GB, ext4 on `/dev/sda1`) with nothing else running, PostgreSQL 18.7 in Docker with default settings (`fsync=on`, `synchronous_commit=on`, `wal_sync_method=fdatasync`, `shared_buffers=128MB`, `max_wal_size=1GB`), recreated before each library. Five rounds of every test, libraries one after another in alternating order, plus a traced round per Runwick test kept out of the numbers. Jobs are 1 KiB no-ops. Libraries: Runwick, River 0.50.0, pg-boss 12.38.0, Wolverine 6.65.1 (PostgreSQL transport), BullMQ .NET 1.3.7 (PostgreSQL backend). The floor is one `INSERT` of 1 KiB with a commit per write.

Raw rounds, logs and manifests: [`evidence/baseline-v0.1.5/postgres/`](evidence/baseline-v0.1.5/postgres/).

## Results

Jobs per second, median of 5 rounds, min to max in brackets. `*5` means five rounds were incomplete.

| Test | Floor | Runwick | River | pg-boss | Wolverine | BullMQ .NET |
|---|---:|---:|---:|---:|---:|---:|
| Add one, 1 caller | 366 (144–393) | 75 (61–79) | 204 (169–224) | 213 (178–253) | 86 (79–109) | 229 (222–346) |
| Add one, 16 callers | 2,226 (1,433–3,116) | 508 (461–590) | 1,629 (1,180–1,869) | 1,114 (802–1,916) | 807 (329–1,013) | 628 (509–828) |
| Add one, 64 callers | | 672 (557–705) | 1,948 (1,678–2,390) | 1,827 (1,493–2,658) | 1,336 (509–1,792) | 660 (578–846) |
| Bulk, native chunk | | 1,094 (948–1,158) | 17,502 (15,683–19,043) | 15,018 (13,009–17,667) | no bulk call | 10,294 (9,430–11,453) |
| Bulk, 100 per call | | 1,143 (1,132–1,383) | 9,846 (6,972–11,852) | 8,471 (4,898–8,693) | no bulk call | 4,832 (4,416–5,820) |
| Drain, 16 slots | | 380 (247–401) | 1,890 (1,850–2,407) | 1,184 (912–1,361) | 70 (65–71) | 552 (308–656) |
| Drain, 64 slots | | 715 (541–986) | 5,462 (3,914–6,624) | 2,325 (1,408–3,167) | 70 (70–71) | 567 (521–658) |
| Steady 100/s | | 100 (99–100) | 98 (96–100) | 98 (96–100) | 70 (68–71) | 98 (31–99) |
| Steady 500/s | | 295 (246–339) | 422 (355–462) | 435 (384–459) | 71*5 (69–72) | 114 (93–146) |

"Native chunk" is each library's own bulk size: 100 for Runwick (PostgreSQL accepts up to 1,000 but the harness default stays at 100), 1,000 for the others. Steady figures are jobs finished per second including catch-up. Wolverine's five steady-500 rounds are incomplete: after the 60-second grace period roughly 4,200 to 6,300 of about 11,000 to 12,600 offered jobs were still unfinished. Every Runwick, River, pg-boss and BullMQ .NET round reconciled with the database.

### Cost per job, timed phase only (median)

CPU is the application plus PostgreSQL. WAL is bytes written to the write-ahead log, read from the server's log position, which is exact.

| Test | Library | CPU ms/job | WAL bytes/job | Wire bytes/job | p99 ms |
|---|---|---:|---:|---:|---:|
| Add one, 1 caller | Runwick | 14.1 | 4,320 | 3,464 | 67 |
| | River | 1.6 | 1,832 | 3,313 | 22 |
| | pg-boss | 3.4 | 1,688 | 4,613 | 20 |
| Add one, 16 callers | Runwick | 9.5 | 4,335 | 3,430 | 113 |
| | River | 1.4 | 1,837 | 3,312 | 55 |
| | pg-boss | 2.5 | 1,799 | 4,617 | 112 |
| Bulk, 100 per call | Runwick | 1.06 | 3,435 | 1,644 | |
| | River | 0.05 | 1,784 | 2,306 | |
| | pg-boss | 0.11 | 1,583 | 1,074 | |
| Drain, 16 slots | Runwick | 5.2 | 7,621 | 3,804 | |
| | River | 0.7 | 3,983 | 2,638 | |
| | pg-boss | 0.7 | 3,027 | 1,761 | |
| Drain, 64 slots | Runwick | 3.5 | 8,050 | 3,617 | |
| | River | 0.3 | 3,991 | 2,597 | |
| | pg-boss | 0.4 | 3,126 | 1,575 | |

Runwick writes roughly twice the log per job in both adds and drains. Wire bytes are similar on adds and about 40% higher on drains, so most of the extra cost looks to be on the server side (more rows, indexes and events written per job) rather than more data sent. That is a reading of the numbers, not a measurement of where it goes. The CPU per job is 7 to 20 times River's on adds and bulk. I have not attributed that to the client or the server; the plan's Stage 2 experiment does that.

## Against the targets

Target = 90% of the fastest other library in the same column on this host. Sizing figures only.

| Test | Runwick now | 90% target (leader) | Needed |
|---|---:|---:|---:|
| Add one, 1 caller | 75 | 206 (BullMQ .NET) | 2.7× |
| Add one, 16 callers | 508 | 1,466 (River) | 2.9× |
| Add one, 64 callers | 672 | 1,753 (River) | 2.6× |
| Bulk, native | 1,094 | 15,752 (River) | 14× |
| Bulk, 100 per call | 1,143 | 8,861 (River) | 7.8× |
| Drain, 16 slots | 380 | 1,701 (River) | 4.5× |
| Drain, 64 slots | 715 | 4,916 (River) | 6.9× |

The throughput plan names pg-boss as the leader for single-caller add, bulk and 16-slot drain, and River for 16-caller add and 64-slot drain. On this host River leads almost every column. The sizes are of the same order as the plan's November figures.

**Cost budget.** The bare insert-and-commit costs about 2,730 µs per write on this disk (366/s). Runwick's single add takes about 13,300 µs, and River's takes about 4,900 µs. So this disk's commit already uses over half of River's time per add, which is why single-caller PostgreSQL numbers here say more about the disk than the library.

## Caveats

1. **Slow disk, one host.** (A second run with PostgreSQL's data on memory gave the same Runwick single-caller rate, 78/s against 75/s, so the commit time is not what limits it: see the [fast-disk report](queue-baseline-v0.1.5-fast-disk.md).) Commits cost about 2.7 ms here, which squeezes the single-caller rows. The shapes (who scales with callers, who is bound by work per job) carry over better than the absolute numbers. A second host is still needed before any public claim.
2. **Bulk at 20,000 jobs on a fresh database may be hitting the known first-load validation plan problem** described in the throughput plan: a statement trigger that scans unrelated rows until statistics exist, fixed in the investigation by an `ANALYZE`. The Runwick bulk rate of about 1,100/s is the same one the plan reports (1,106/s). This run does not separate that effect from other costs.
3. **Steady 500/s overloads Runwick and Wolverine.** Runwick's p99 there is about 20 seconds and Wolverine's about 73. The offers were made on a fixed schedule, so those numbers are what a producer at that rate would see.
4. **Wolverine's PostgreSQL drain is bound by its listener wait, not by the work.** Seventy per second at 16 and 64 slots, in every round, with a 250 ms wait after each batch of 20.

## How noisy these numbers are

On this machine most cells vary by more than 15% between the lowest and highest of five rounds (PostgreSQL: 36 of 45 cells; SQLite: 17 of 26). The disk is the reason: even the bare write ranges widely (PostgreSQL one-caller floor 144 to 393/s). I did not re-run these cells, because more rounds on the same disk would tighten the medians but not the spread. The gaps that drive the conclusions (2 to 14 times for PostgreSQL, 2 to 4 times for most SQLite rows) are much larger than the spread. The small SQLite gaps (bulk at native chunk size, 1.14 times, and 16 or 64 callers, 1.35 to 1.5 times) are inside it and should be re-measured on a quieter disk before anyone acts on them. The Redis run on CI had no cell above 15%.

## Re-runs

- Seven River rounds failed on the first pass with `connection reset by peer` while connecting (six single-add rounds and one bulk round), each at the start of a test. I did not find the cause; the store had just been recreated and the same cells ran cleanly the second time. Those seven were re-run one at a time against a freshly started PostgreSQL. The failed first-pass files are in [`rerun-superseded/postgres/`](evidence/baseline-v0.1.5/rerun-superseded/postgres/). Treat River's numbers for those cells as measured under better conditions than the rest of its rounds, which may flatter it slightly.

## Reproduce

```bash
python3 eng/bench-compare/ci/run.py --systems writefloor-pg,runwick-pg,river,pgboss,wolverine-pg,bullmq-dotnet-pg \
  --workload normal --rounds 5 --fresh-stores --trace-round --out results
```
