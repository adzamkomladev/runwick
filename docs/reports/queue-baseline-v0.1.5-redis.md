# Redis baseline for v0.1.5

24 November 2026 · Runwick v0.1.5 (`src/` identical to the tag; main at `130f87c`) · [GitHub run 45391519212](https://github.com/adzamkomladev/runwick/actions/runs/45391519212) · part of [Stage 1](../plans/queue-throughput-stage-1-plan.md) of the [throughput plan](../plans/queue-throughput-engineering-plan.md). PostgreSQL and SQLite baselines are still to come.

## In short

On one GitHub runner, with Redis 8.2.11 set to the same strict durability for everyone (AOF, `appendfsync always`), Runwick's Redis store is well behind the fastest Redis queues in every test except steady arrival rates, where everything keeps up.

- **Adding one job at a time:** Runwick tops out at about 2,100 jobs/s no matter whether 16 or 64 callers are adding. The others reach 6,700 to 14,200 and keep growing with more callers. With one caller Runwick does 656/s against 1,300 to 1,570.
- **Adding in bulk:** about 5,100 jobs/s, against 9,000 to 13,150.
- **Processing a backlog:** with the default settings, 1,921 jobs/s at 16 slots and 3,066 at 64. With the already-existing "short job" worker settings, 3,110 and 3,852. The peers do about 5,000 to 5,500 and 6,600 to 7,400.
- **The cause is mostly work per job, not the disk.** Runwick makes Redis run about 21 commands per accepted job (Wolverine: 1, BullMQ: 9) and moves about 2,900 bytes per single add (peers: 1,500 to 2,000).
- **Flagged rounds explained:** every Runwick round on Redis is flagged "incomplete" because the store's count came up 1 to 3 jobs short. That was a bug in Runwick's Redis job statistics, not lost jobs; fixed after the baseline. See [Resolved](#resolved-the-store-count-was-1-to-3-jobs-short).

## What was run

One runner (ubuntu-24.04, 4 cores, Intel Xeon Platinum 8573C). The Redis container was thrown away and recreated before each library, and the libraries ran one after another, in the opposite order on even rounds. Five rounds per test, plus one extra traced round for each Runwick test that is kept out of the numbers. Every library used the same server settings (the full list is in the manifest: [manifest](evidence/baseline-v0.1.5/ci-redis/)). Jobs are 1 KiB no-ops.

Libraries: Runwick (default), Runwick with the tuned worker settings (`--buffered-finish --claim-ahead 16 --claims-in-flight 2`), Wolverine, BullMQ for .NET, BullMQ for Node. "Floor" is the bare store: one `XADD` of 1 KiB with nothing else, so it shows what the disk and Redis cost before any queue exists.

Raw results, logs and the manifest: [`evidence/baseline-v0.1.5/ci-redis/`](evidence/baseline-v0.1.5/ci-redis/) (766 files; every round, including incomplete ones).

## Results

Jobs per second, median of 5 rounds, min to max in brackets. `*` = some of that cell's rounds are flagged incomplete (all Runwick; explained and resolved below). The throughput comes from the harness's own count in both cases.

| Test | Floor | Runwick | Runwick tuned | Wolverine | BullMQ .NET | BullMQ Node |
|---|---:|---:|---:|---:|---:|---:|
| Add one, 1 caller | 1,875 (1,628–1,989) | 656* (646–691) | 670* (647–679) | 1,405 (1,382–1,428) | 1,287 (1,208–1,344) | 1,569 (1,485–1,614) |
| Add one, 16 callers | 15,857 (13,576–15,960) | 2,133* (2,078–2,176) | 2,136* (2,107–2,184) | 10,457 (10,438–10,691) | 6,727 (6,321–7,009) | 8,369 (8,135–8,415) |
| Add one, 64 callers | | 2,133* (2,107–2,173) | 2,150* (2,129–2,175) | 14,198 (14,058–14,486) | 11,917 (11,769–12,430) | 13,182 (12,899–13,533) |
| Bulk add, native chunk | | 5,146* (4,964–5,360) | 5,004* (4,890–5,183) | no bulk call | 10,496 (9,979–10,597) | 13,150 (12,861–13,414) |
| Bulk add, 100 per call | | 5,053* (4,887–5,307) | 5,134* (4,985–5,273) | no bulk call | 9,050 (8,934–9,412) | 12,210 (12,145–12,674) |
| Drain, 16 slots | | 1,921* (1,867–1,948) | 3,110* (3,040–3,214) | 5,283 (4,744–5,438) | 5,003 (4,803–5,204) | 5,487 (5,391–5,523) |
| Drain, 64 slots | | 3,066* (2,981–3,150) | 3,852* (3,722–4,011) | 7,289 (7,051–7,442) | 6,617 (6,428–7,215) | 7,396 (7,389–7,763) |
| Steady 100/s | | 100 | 100 | 99 | 100 | 100 |
| Steady 500/s | | 500* | 499* | 493 | 499 | 500 |

"Native chunk" is each library's own bulk size: 100 for Runwick, 1,000 for the BullMQs. The Wolverine drain figures above count from worker start. Its rate from first claim is much higher (7,776 at 16 slots, 14,524 at 64) because it spends about a second compiling handlers before it claims anything. Runwick's two clocks are the same, because its worker starts claiming at once.

### Cost per job, timed phase only

Median over rounds; seeding, startup and shutdown are left out. "Commands" is how many commands Redis executed per job, including those inside scripts.

| Test | Library | CPU ms/job | Wire bytes/job | Redis commands/job |
|---|---|---:|---:|---:|
| Add one, 1 caller | Runwick | 1.73 | 2,876 | 21.0 |
| | Wolverine | 0.65 | 1,967 | 1.0 |
| | BullMQ .NET | 0.78 | 1,598 | 9.0 |
| | BullMQ Node | 0.35 | 1,570 | 9.0 |
| Bulk, 100 per call | Runwick | 0.27 | 1,888 | 4.2 |
| | BullMQ .NET | 0.24 | 1,580 | 9.0 |
| | BullMQ Node | 0.09 | 1,430 | 9.0 |
| Drain, 16 slots | Runwick | 0.83 | 3,904 | 13.1 |
| | Runwick tuned | 0.63 | 3,711 | 8.8 |
| | Wolverine | 0.42 | 1,877 | 1.1 |
| | BullMQ .NET | 0.35 | 2,032 | 27.9 |
| | BullMQ Node | 0.24 | 2,065 | 27.9 |

CPU is app plus store. Two readings: in bulk Runwick runs fewer Redis commands per job than BullMQ (4 against 9) yet is about 40% as fast, and BullMQ runs more commands than Runwick while draining but is still much faster, so command count alone is not the story. The AOF growth figures are left out: they went negative when Redis rewrote its log mid-run.

## Against the targets

Target = 90% of the fastest other library in the same column on the same host. These are sizing figures; a release decision uses paired runs.

| Test | Runwick now | 90% target (leader) | Needed |
|---|---:|---:|---:|
| Add one, 1 caller | 656 | 1,412 (BullMQ Node) | 2.2× |
| Add one, 16 callers | 2,133 | 9,412 (Wolverine) | 4.4× |
| Add one, 64 callers | 2,133 | 12,778 (Wolverine) | 6.0× |
| Bulk add, native | 5,146 | 11,835 (BullMQ Node) | 2.3× |
| Bulk add, 100 per call | 5,053 | 10,989 (BullMQ Node) | 2.2× |
| Drain, 16 slots (default → tuned) | 1,921 → 3,110 | 4,939 (BullMQ Node) | 2.6× → 1.6× |
| Drain, 64 slots (default → tuned) | 3,066 → 3,852 | 6,656 (BullMQ Node) | 2.2× → 1.7× |

The throughput plan names Wolverine and BullMQ .NET as the leaders for Redis and treats BullMQ Node as a separate target. Using only those two, the 1-caller target falls to 1,265 (Wolverine), bulk to 9,447 (BullMQ .NET) and drain to 4,755 and 6,560. The gaps stay about the same size.

**Cost budget for one caller.** At the 1,412/s target one synchronous caller has 708 µs per accepted job. The bare write already costs 533 µs on this runner, leaving 175 µs for everything Runwick does. Runwick spends about 1,520 µs today. The single-caller target is only reachable if Runwick's per-job work comes down to a small multiple of the bare write, which is why Wolverine (one command, 711 µs) is the right yardstick.

## What this says about where to look

This is a reading of the numbers, not a diagnosis; Stage 2 is the experiment that tests it.

- **Producer path:** the flat 2,133/s from 16 to 64 callers points at something serial in Runwick's single add (one script per job doing about 21 Redis commands), not the disk, since the bare write reaches 15,857/s at 16 callers.
- **Bulk path:** commands per job are low but throughput is still about 40% of BullMQ Node, so look at time per call (the batch script and the client work around it), not command count.
- **Worker:** the tuned profile closes about a third of the gap at 16 slots (1,921 → 3,110 against about 5,300) and about a fifth at 64 (3,066 → 3,852 against about 7,000). Most of the gap is still open.

## Resolved: the store count was 1 to 3 jobs short

70 Runwick Redis rounds were marked incomplete, 35 per arm: bulk, bulk-100, add-one at 16 and 64 callers, both drains and steady-500, in all 5 rounds each. The store count came up 1 to 3 jobs short (for example 19,997 of 20,000), and the shortfall was already there before the worker started.

**Cause: a bug in Runwick's Redis job statistics, not lost jobs.** Redis held all 20,000 jobs (20,000 job records and 20,000 entries in every index). The statistics read walks the creation index 5,000 entries at a time, handing back a cursor at each page boundary. The cursor named the next job not yet counted, but the next page skipped everything up to and including that id, so one job was dropped per boundary: 3 at 20,000 jobs, 1 at 10,000, and none below 5,000. The management dashboard and API stats on a Redis store with more than 5,000 jobs were therefore off by one per 5,000 jobs. SQLite and PostgreSQL were never affected.

**Fix and check:** one comparison in `src/Runwick.Redis/Lua/ops/mread.lua`, with a new contract test (JST-06, 5,300 jobs on every store) that failed on Redis before the fix (5,299 of 5,300) and passes after. A local 20,000-job bulk run and drain now count exactly 20,000. The four other Lua scans with the same shape keep the last examined job as their cursor, where the skip is correct.

**What it means for the numbers above:** the throughput figures were always from the harness's own counts and are unaffected. The 70 rounds stay flagged incomplete in the raw results because that is what the checker said at the time; read them as complete for Runwick Redis. The baseline was measured on code identical to v0.1.5; the statistics fix lands after it, so a re-run on current main would show these rounds as complete. I did not re-run on GitHub.

## Caveats

- One runner type, one host, one day. The November CI figures came from different machines and are not comparable (Runwick single add was 1,084 then, 656 now; Wolverine 2,807 then, 1,405 now).
- Seven matched pairs and a second host are still needed before a public claim, per the throughput plan.
- The manifest records Redis `save` as the text "save" where it should be empty (a bug in how the driver reads that setting); the live setting is "no snapshots".
- The write floor is a bare `XADD`. Runwick's acknowledgement covers more (listing indexes, receipt, event), so some gap to the floor is expected; how much is what the next stages measure.

## Reproduce

```bash
python3 eng/bench-compare/ci/run.py --systems writefloor-redis,runwick-redis,runwick-redis-tuned,wolverine-redis,bullmq-dotnet-redis,bullmq \
  --workload normal --rounds 5 --fresh-stores --trace-round --out results
python3 eng/bench-compare/ci/report.py results report
```

Or in GitHub: Actions → bench → Run workflow, layout `same-host`, stores `redis`, rounds 5.
