# Stage 2, PostgreSQL: the dispatch SQL function is the cost, and claim batching is the drain lever

26 November 2026 · Runwick v0.1.5 behaviour · PostgreSQL 18.7 in Docker, data on `tmpfs`, default durability settings · 8 shared vCPU AMD EPYC · [overview and method](queue-stage2-overview.md) · raw rounds in [`evidence/stage2/postgres-tmpfs/`](evidence/stage2/postgres-tmpfs/) and [`postgres-tmpfs-trace/`](evidence/stage2/postgres-tmpfs-trace/) (per-function server time), text in `summary-postgres.txt`

## Numbers

### Single add, one caller (median of 5 rounds, range in brackets)

| Arm | ms per add | Jobs/s | Process CPU ms | PostgreSQL container CPU ms | Transactions per add |
|---|---:|---:|---:|---:|---:|
| Public `DispatchAsync(job)` | 12.15 (11.41–12.85) | 82 | 3.86 | 8.14 | 1.1 |
| Preparation alone | 0.033 (0.028–0.263) | | 0.043 | 0 | |
| A command prepared before the timer | 11.56 (10.40–16.75) | 87 | 3.44 | 7.97 | 1.1 |
| The bare store call | 11.12 (10.70–13.10) | 90 | 3.27 | 7.91 | 1.1 |
| Receipt replay (same command sent again) | 6.97 (6.17–7.31) | 143 | 2.88 | 2.96 | 2.1 |

Preparation is 0.3% of a single add; Runwick's store layer (dispatcher plus receipt handling, public minus prepare minus store call) adds 1.0 ms (8%). Almost all of the rest is one call to PostgreSQL that takes 11 ms and uses about 8 ms of the container's CPU. For comparison the plain 1 KiB insert on memory takes 1.9 ms at one caller (514/s) and reaches 5,430/s at 16 callers ([fast-disk baseline](queue-baseline-v0.1.5-fast-disk.md)).

Inside PostgreSQL, from a separate traced run (`track_functions = all`, 2 rounds of 600 adds; per add):

| Function | ms | Share of the function total |
|---|---:|---:|
| `dispatch_v4` (the whole call) | 4.2–4.4 | 100% |
| `rw_dispatch_core_v4` | 2.6–2.9 | about 65% |
| `rw_frame_v1` (request framing, replay and conflict checks) | 1.0–1.1 | about 25% |
| `rw_receipt_v1` | 0.57–0.63 | 14% |
| `rw_gate_v1` + `storage_commands_assert_decided` + `rw_finalize_v1` | 0.39–0.44 + 0.39–0.42 + 0.33–0.36 | about 27% together |

The percentages overlap because the functions nest. Per add the store writes about **4.1 to 4.4 KB of WAL** and 5 to 10 rows. The function's 4.2 ms is about half of the 8 ms of container CPU per add; the other half is outside functions (commit, WAL writes, the other server processes in the container), and this run cannot split it further.

### Many callers (5 rounds, 3,000 adds per arm; jobs/s)

| Callers | Public | Prepared first | Bare store call | PostgreSQL CPU per add |
|---:|---:|---:|---:|---:|
| 16 | 571 (458–607) | 565 (482–724) | 592 (537–639) | 7.0–7.2 ms |
| 64 | 490 (475–588) | 509 (458–668) | 526 (471–632) | 6.2–6.4 ms |

All three arms land together. At 16 callers PostgreSQL is using about four cores for 570 adds/s. Preparing first does not help (565 vs 571). Going from 16 to 64 callers does not help either.

### Bulk add, 100 jobs per call (median of 5 rounds)

| Arm | ms per call | Jobs/s | Process CPU ms | PostgreSQL CPU ms | Allocated |
|---|---:|---:|---:|---:|---:|
| Public `DispatchManyAsync` | 54.6 (50.1–61.0) | 1,830 | 14.9 | 47.3 | 3.5 MB |
| Preparation alone | 2.2 (1.5–8.1) | | 3.1 | 0 | 2.7 MB |
| Prepared first | 50.9 (42.3–61.2) | 1,960 | 9.1 | 45.2 | 0.8 MB |
| The bare store call | 54.1 (42.3–61.2) | 1,850 | 6.6 | 45.7 | 0.8 MB |

Preparation is 4% of the call. Per batch of 100, `dispatch_v4` takes 29.7 ms, of which `rw_dispatch_core_v4` is 25.3 ms (0.25 ms per job), and the store writes 341 KB of WAL (3.4 KB per job, 2.7 rows per job) in 2 to 3 transactions. PostgreSQL uses 0.46 ms of container CPU per job. At 0.46 ms of PostgreSQL CPU per job, River's 15,752 jobs/s bulk rate (this host's baseline) would need about seven of this machine's eight cores for PostgreSQL alone, so the SQL work per job has to shrink, not just be batched differently.

### Drain, 20,000 ready jobs (jobs/s, median and range)

| 16 slots | Rounds | Jobs/s | Claim size | Finish size | PostgreSQL CPU per job | p99 handler return to confirm |
|---|---:|---:|---:|---:|---:|---:|
| Public worker | 5 | 376 (307–465) | 8.5 | 16.9 | 3.59 ms | 191 ms |
| Direct driver, same sizes | 5 | 484 (432–525) | 9.0 | 14.3 | 2.97 ms | 175 ms |
| Direct, flush 0 ms / 20 ms | 3 / 3 | 482 / 430 | 9.0 | 11.0 / 16.7 | 3.10 / 3.18 ms | 140 / 207 ms |
| Public, claim ahead 16 | 3 | **757 (723–807)** | 17.0 | 29.2 | 2.09 ms | 192 ms |
| Public, 2 claims in flight | 3 | **721 (649–724)** | 8.1 | 25.3 | 2.91 ms | 226 ms |

| 64 slots | Rounds | Jobs/s | Claim size | Finish size | PostgreSQL CPU per job |
|---|---:|---:|---:|---:|---:|
| Public worker | 3 | 1,087 (1,057–1,250) | 32 | 32 | 1.75 ms |
| Direct driver, same sizes | 3 | 918 (790–1,053) | 32 | 32 | 1.60 ms |
| Public, claim ahead 32 | 3 | 1,075 (922–1,124) | 32 | 32 | 1.74 ms |
| Public, 2 claims in flight | 3 | **1,806 (1,229–1,892)** | 31.2 | 31.6 | 1.73 ms |

At 64 slots the public worker already claims and finishes 32 at a time, so claim-ahead changes nothing; a second claim in flight is what moves it (+66%, ranges touching). One direct-driver round stalled for several seconds (p99 5.6 s), which is why that pair is not read as the direct driver being slower. The p99 from handler return to confirmed finish is 130 to 230 ms at every setting that was tried, much longer than Redis's 30 to 55 ms, because a finish batch waits for a commit that costs milliseconds of PostgreSQL CPU.

## First attributable cost, and what would falsify it

**Single and bulk add:** the dispatch SQL function and its WAL. A single add costs 11 ms of which the .NET side is about 1.1 ms (preparation 0.03, store layer 1.0); the call itself is the rest: 4.2 ms inside `dispatch_v4` with 4 KB of WAL, and about 7 ms outside the function (round trip, commit, process wake-ups) that this run does not split. The public dispatcher reaches 93% of the bare store call at 64 callers.

**What would falsify it:** a hand-written minimal SQL for the same durable work (one job row, one receipt row, the same unique constraints and indexes, no framing/gate/finalize steps) called from the same connection pool at 16 callers. If it is within 1.3× of today's 592/s the cost is not in the function body (look at commit and WAL instead). If it is more than 3× faster (the bare insert reaches 5,430/s) the function body is the cost. A traced run of the same minimal SQL with `track_functions` would also say whether the 3.6 ms of container CPU outside functions shrinks with it.

**Drain:** the worker's claim batching and concurrency, not its code. At 16 slots the public worker claims 8.5 jobs per call. Claiming ahead of free slots (17 per call) doubles the rate (376 to 757) and cuts PostgreSQL CPU per job by 42% (3.59 to 2.09 ms). The direct driver at the same batch sizes is 1.29× faster than the public worker (484 vs 376, ranges 432–525 vs 307–465, partly overlapping), so a smaller engine overhead may exist on top; at 64 slots that gap reverses (918 vs 1,087), so it is not established.

**What would falsify it:** if the public worker with claim ahead 16 does not beat the matched default by at least 1.5× over seven pairs, or if the direct driver at 16 slots with claim size 17 and finish size 29 (the claim-ahead sizes) does not match 757, the scheduling diagnosis is wrong and something other than batch size is limiting.

## Next action by the plan's decision table

| Observation | Action |
|---|---|
| Preparation is 0.3% (single) and 4% (bulk) | No shared-serialization work; Stage 3 is purely the PostgreSQL function body and WAL. |
| Public and direct at matched sizes: 1.29× at 16 slots, reversed at 64, ranges overlap | Not enough to blame the engine. Repeat with seven pairs before tracing ticket and lane handoffs. |
| Throughput separates only when claim timing or in-flight claims change (2.0× and 1.9× at 16 slots, 1.66× at 64) | Stage 4: scheduling-policy change (default claim ahead or two claims in flight for PostgreSQL); check pickup, lease margin and fairness with competing workers. |
| Counters name the expensive operation (4.2 ms of function time and 4 KB of WAL per add, 1.6 to 3.6 ms of server CPU per drained job) | Stage 3 PostgreSQL producer and Stage 4 PostgreSQL worker SQL. |

## Not checked

Disk-backed PostgreSQL (commit and WAL fsync cost), several worker processes, multiple lanes, a minimal-SQL control (the falsifying experiment), the 3.6 ms of container CPU outside the function, fewer than seven pairs, EventPipe CPU and allocation traces. The traced function run has 2 rounds only. PostgreSQL CPU is the container's whole cgroup (includes background processes and idle wake-ups), not per-backend CPU.
