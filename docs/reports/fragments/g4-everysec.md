# Redis against BullMQ with `appendfsync everysec` (G4)

## What we wanted to know

Runwick's `AofEverySecond` profile was measured before, but only against Runwick's own always-fsync profile. The open question was how Runwick compares with BullMQ when both run under everysec, which is what most BullMQ users run.

## How it was run

- Script: `eng/bench-compare/redis-group/pairs-combined.sh`, which now takes `APPENDFSYNC=everysec`. That starts every Redis with `--appendfsync everysec` and passes `--redis-everysec` to the Runwick arms.
- Arms: `base` (Runwick plain), `buf` (Runwick with buffered finish), `bullmq`.
- 20,000-job drains: 5 rounds under everysec, and 3 rounds under always in the same session as a control.
- Steady 500 jobs/s offered: 5 rounds under everysec.
- All 39 runs exited cleanly. Results are in `artifacts/perf/round-two/g4-everysec/`.

## Drain throughput (median jobs/s)

| Arm | everysec | always |
|---|---|---|
| Runwick base | 817 | 431 |
| Runwick buf | 1,178 | 650 |
| BullMQ | 3,183 | 1,275 |

Per-round ratios against BullMQ:

| | Rounds | buf / BullMQ | base / BullMQ |
|---|---|---|---|
| everysec | 1–5 | 0.46, 0.36, 0.43, 0.39, 0.37 | 0.31, 0.22, 0.45, 0.21, 0.30 |
| always | 1–3 | 0.57, 0.34, 0.51 | 0.37, 0.30, 0.34 |

- Everysec speeds up every arm: Runwick base about 1.9×, buf about 1.8×, BullMQ about 2.5×.
- BullMQ gains the most, so the gap gets a little wider. Under everysec, BullMQ drains about 2.7× faster than Runwick buf and about 3.9× faster than base.

## Bytes written to the AOF per job

| Arm | everysec | always |
|---|---|---|
| Runwick base | 5,321 | 5,300 |
| Runwick buf | 5,046 | 5,055 |
| BullMQ | 2,815 | 2,815 |

The fsync setting doesn't change how much is written. Runwick still writes about 1.8–1.9× more per job than BullMQ, and that matches the throughput gap seen under always fsync. It points at the same cause as before: Runwick's job record and its per-job bookkeeping keys, not the fsync cost.

## Steady 500 jobs/s offered, everysec (medians of 5 rounds)

| Arm | completions/s | offer-to-finish p50 | p99 |
|---|---|---|---|
| Runwick base | 488 | 64 ms | 1,119 ms |
| Runwick buf | 494 | 47 ms | 552 ms |
| BullMQ | 487 | 8 ms | 144 ms |

- All three keep up with the offered rate.
- Runwick's latency is much higher and swings more from round to round. One base round had p50 1,211 ms and p99 2,678 ms, and one buf round had p99 1,472 ms.
- Both sides turned away some offers at the producer's local limit: Runwick 41–723 per round (`local_capacity_exceeded`; median base 301, buf 158), BullMQ 52–766 (median 398).

## Caveats

- The host was busy. The 1-minute load at the start of runs ranged from 0.97 to 12.1, and several drain rounds started above 8. The runner only waited for low load before each set, not before each round. The round-to-round spread in the ratios above reflects this.
- BullMQ's latency figure comes from its `latencyMs` field, which we take to mean the same offer-to-finish time.

## What this means

Everysec doesn't change the picture: Runwick is about 2.5–4× behind BullMQ on raw Redis drain speed, under either fsync setting. Buffered finish narrows the gap more than any other setting we have. The remaining gap is about bytes and commands per job, which is the lean-path work (work area F), not durability settings.
