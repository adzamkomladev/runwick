# Stage 2: where does Runwick's queue time go? (overview)

26 November 2026 · Runwick v0.1.5 behaviour (`src/` unchanged; only test tooling was added) · [Stage 2 of the throughput plan](../plans/queue-throughput-engineering-plan.md#stage-2--isolate-the-shared-engine) · reports per store: [Redis](queue-stage2-redis.md) · [PostgreSQL](queue-stage2-postgresql.md) · [SQLite](queue-stage2-sqlite.md)

## The answer in one table

Stage 2 asked: is the gap to the fastest libraries Runwick's own .NET work, its worker, or the store? On this machine it is mostly **the store call**, plus **worker scheduling settings** for draining. It is not the shared preparation code.

| | Redis | PostgreSQL | SQLite |
|---|---|---|---|
| Share of one single add spent preparing the job in .NET (serialize, validate, fingerprints) | 1.5% (0.035 of 2.38 ms) | 0.3% (0.033 of 12.2 ms) | 3.4% (0.056 of 1.64 ms) |
| Share of a 100-job bulk add spent preparing | 13% (3.3 of 26.1 ms) | 4% (2.2 of 54.6 ms) | 12% (4.2 of 34.3 ms) |
| Public dispatch compared with the bare store call, one caller | 1.14× slower | 1.09× slower | 1.29× slower |
| Same, 64 callers (jobs/s) | 1,772 vs 1,907 raw (0.93×) | 490 vs 526 raw (0.93×) | 1,111 vs 1,173 raw (0.95×) |
| Drain, 16 slots: public worker vs a minimal direct driver at the same batch sizes | 948 vs 1,057 (direct 1.11×, noisy) | 376 vs 484 (direct 1.29×) | 462 vs 475 (parity) |
| Drain, 16 slots: best one-knob change to the public worker | claim ahead 16: 1,288 (+36%) | claim ahead 16: 757 (+101%) | none beyond noise |
| Drain, 64 slots: public vs direct | 1,335 vs 1,312 (parity) | 1,087 vs 918 (public ahead, noisy) | 492 vs 593 (direct 1.2×, ranges overlap) |
| Drain, 64 slots: best one-knob change | claim ahead 32: 1,773 (+33%) | two claims in flight: 1,806 (+66%) | none beyond noise |
| **First attributable cost** | the add script inside Redis: 21 commands, 0.5 to 0.7 ms of the Redis thread per add | the `dispatch_v4` SQL function: 4.2 ms of 11 ms per single add, about 4 KB of WAL per add | the single-writer transaction body in the SQLite store call (1.27 ms of 1.64 ms single, 0.32 ms per bulk job) |
| **Next action (plan's decision table)** | Stage 3 Redis producer (script work per add); Stage 4 scheduling policy for drain | Stage 3 PostgreSQL producer (the function body); Stage 4 scheduling policy first, worker trace second | Stage 3 SQLite producer and Stage 4 SQLite worker are both store work; no worker change indicated |

All rates are medians of 3 to 5 rounds with the range in the store reports. Everything ran on memory-backed storage (see below), one store at a time, one caller or fixed callers at a time.

## What was measured

**Experiment A: dispatch.** For each store, one caller at a time with fresh operation and job IDs every time, identical 1 KiB payloads and options, five arms measured separately:

1. the public `DispatchAsync(job)` entry point;
2. preparation alone (`Prepare`, no store call, its own timer);
3. dispatch of a command prepared before the timer started;
4. the store call alone with a prebuilt store command (no dispatcher, no admission, no receipt translation);
5. a second send of an already-accepted command (receipt replay).

Each was done for fresh single jobs and for fresh batches of 100, five rounds, arm order rotated every round. Each arm records time per operation, the process's CPU time and allocated bytes per operation, the store container's CPU time (cgroup), and the store's own counters per operation (Redis commands, PostgreSQL transactions, rows, WAL bytes, per-function time). Preparation was also split into serialization, validation and intent, and fingerprints. At 16 and 64 callers the arms 2 to 4 were repeated to see whether the one-caller picture holds under load.

For Redis, a raw replay was added: the exact script call one real add makes was captured and sent again from a plain StackExchange.Redis connection with a fresh command ID, deadline and job ID (every copy of the ID, including the one inside the item JSON), with no Runwick code in the loop. It was compared with a bare `PING` (round trip alone) and an `ECHO` of the same size (round trip plus moving the bytes).

**Experiment B: worker.** For each store and 16 and 64 slots, an identical seeded backlog of 20,000 ready 1 KiB jobs in a fresh store, drained by the public worker and by a minimal direct driver that calls the same provider methods (ReserveAndBegin, then Finish or FinishMany), with the same no-op handler, slots, lease length (300 s), resource-usage setting (off in both) and the same call recorder in front of the store. First the public worker ran once to record its actual mean claim and finish batch sizes; the direct driver was then set to those sizes (the "matched pair", 5 rounds at 16 slots, 3 at 64, alternating order). Then one knob changed at a time: public worker with claim-ahead and with two claims in flight; direct driver with finish flush delay 0 ms and 20 ms (matched 5 ms). Time runs from the first claim to the last confirmed finish; startup is reported apart. A drain only counts if the store confirmed all 20,000 finishes.

## How far to trust it

- **Storage class.** Everything used memory-backed storage (`--store-tmpfs`: Docker `tmpfs` for PostgreSQL and Redis data, `TMPDIR=/dev/shm` for SQLite), as agreed for timing work on this disk. So commit and fsync cost almost nothing here. The disk-backed picture, and what Redis `appendfsync always` costs on a real disk, is not covered; the [baseline reports](queue-baseline-v0.1.5-fast-disk.md) already show memory leaves Runwick's per-job work unchanged.
- **Quiet machine.** The machine is shared with other people's builds and tests; CPU idle was 5% to 95% over the session. Runs were gated on 80% CPU idle before each run (`eng/bench-compare/stage2.py`, idle logged in every result file). Not every run had that gate: the first Redis dispatch run (75% idle) and the first Redis enqueue and replay runs were made before the gate existed (idle 60% to 93%), and two SQLite runs made at 4% idle were thrown away and repeated. Per-run idle is in each JSON.
- **Rounds.** Five rounds for single-caller and 16-slot pairs, three for 64 slots and one-knob changes. The plan asks for at least seven matched pairs before shipping a claim; none of this is a ship decision, only screening, and ranges are shown wherever they overlap. Where a worker comparison's ranges overlap, treat the difference as unproven.
- **Absolute rates differ from the Stage 1 baselines** by 30 to 40% for single adds on Redis and SQLite (the arms here run after a warm-up in a long-lived process; a fresh `bench --mode enqueue` of 2,000 adds measured 249–308/s on Redis and 305–440/s on SQLite today, against 420 and 611 here). Compare arms with each other, not with the baseline rows.
- **One host, one session.** No second host or disk class.
- **Not done:** the multi-lane check (the harness has one queue), EventPipe traces for PostgreSQL and SQLite (the one Redis trace taken was not supported by the controlled test and is not used as evidence; see the Redis report), allocation traces beyond per-operation allocated bytes, server-side CPU traces of the Redis script by command, and production-like network round trips (loopback through Docker's port forwarding here, about 1 ms per Redis round trip).
- The `Kill_9 DispatchMany60` test was not used.

## Tools added (tests/Runwick.Workloads and eng/, no product code)

- [`Stage2Mode.cs`](../../tests/Runwick.Workloads/Stage2Mode.cs): `stage2 --part dispatch | concurrent | redis-replay`. [`Stage2Worker.cs`](../../tests/Runwick.Workloads/Stage2Worker.cs): `stage2 --part worker --arm public|direct`.
- [`eng/bench-compare/stage2.py`](../../eng/bench-compare/stage2.py): runs a part, gates on idle, alternates arms, skips finished runs so it can be re-run to fill gaps. Summaries: `stage2-summary.py`, `stage2-concurrent-summary.py`, `stage2-worker-summary.py`; EventPipe reader: `trace-top.py`.
- Raw rounds and the text summaries: [`evidence/stage2/`](evidence/stage2/).

Reproduce one store, for example Redis:

```bash
docker run -d --name bench-redis --tmpfs /data -p 127.0.0.1:56392:6379 redis:8.2-alpine redis-server --appendonly yes --appendfsync always --save ""
python3 eng/bench-compare/stage2.py dispatch --store redis --tmpfs --out out/redis
python3 eng/bench-compare/stage2.py worker --store redis --tmpfs --slots 16 --out out/redis/worker16
```
