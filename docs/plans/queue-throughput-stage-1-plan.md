# Stage 1 implementation plan: establish the benchmark

24 November 2026 · implements [Stage 1 of the throughput plan](queue-throughput-engineering-plan.md#stage-1--establish-the-benchmark) · checked against main `6317ff8`

## What Stage 1 has to deliver

A set of v0.1.5 baseline numbers we can trust enough to judge every later change against. "Trust" means three things the November CI run did not give us:

1. **Same machine.** Runwick and the library it is compared with ran on different runners. Targets like "90% of River" need both on one host.
2. **Counted outcomes.** A round only counts if the store agrees with the harness about how many jobs were accepted, finished, failed and left over. Five BullMQ .NET PostgreSQL drains "ran at 33/s" because the harness waited for successes that had actually failed.
3. **Cost of the timed part only.** CPU and bytes per job currently include process startup, seeding the backlog and shutdown.

Stage 1 changes no library code. It is done when the baseline report, its settings manifest and every raw round (including incomplete ones) are published.

## What I found in the code

| Finding | Consequence for the plan |
|---|---|
| `src/` is byte-identical between tag `v0.1.5` and main. Only two files under `tests/` and `eng/` differ. | Pinning the baseline is free: build main. Stage 1 must keep it that way, so add one check that fails if `git diff v0.1.5 -- src` is not empty. |
| Local main is 2 commits behind `origin/main`. The investigation report the plan links to (`queue-benchmark-2026-11-24-investigation.md`) and its evidence only exist on origin. | Step 0 is a fast-forward pull. |
| `BenchMode.cs` already has most of the raw material: `--check` (accepted vs run), `--ledger`, `--trace` (stage gaps, allocations), `--sample-seconds`, `--warmup-jobs`, `--batch-items`, `--producer-calls`, PostgreSQL and Redis counter readers, SQLite commit and statement counters, `completedByEndOfOffers` for steady mode. | Mostly wiring and making things default, not new measurement code. |
| `run.py` runs one library per invocation, samples every 0.5 s across the whole process lifetime, and keeps the command line only in the `.log` file. | It needs a same-host multi-library loop, phase-aware sampling and a manifest. |
| `report.py` drops rounds that are not `ok` and shows medians only. | It needs to show every round and mark incomplete ones. |
| Runwick's drain clock runs first claim → last confirmed finish. Wolverine's starts before the host is built and stops at a handler counter. | Report two clocks for every library: from worker start and from first claim. Keep the old field so published numbers stay comparable. |
| Bulk chunk size is a switch in Runwick, BullMQ Node and pg-boss, and hard-coded to 1,000 in River, BullMQ .NET and Solid Queue. | Add a chunk switch to those three so there is one equal-size bulk row. |
| `eng/bench-compare/ab.sh` already alternates two Runwick builds per round. | Stage 1 does not need a new A/B tool. Stages 3 and 4 reuse `ab.sh`. |
| This machine has 8 cores and about ten unrelated containers running. | Not a quiet host. See decision 1. |

## Two decisions to confirm

**1. Where the baselines run.** Recommended: GitHub runners, but one job per store that runs every library for that store one after another on the same runner. That matches the 4-core machines the targets were sized on, anyone can rerun it, and nothing else is competing for the CPU. Alternative: this machine, which is faster to iterate on but shared. Either way this machine is used for smoke runs while building.

**2. Which competitor harnesses get reworked.** Recommended: only the six that set a target or had a measurement problem: Wolverine, BullMQ .NET, BullMQ Node, River, pg-boss, Solid Queue. Horizon and Celery set no target, so they stay as they are and are not part of the new baseline.

The rest of this plan assumes both recommendations.

## Work packages

### 0. Sync

`git pull --ff-only` to bring in the investigation report and its evidence. Nothing local conflicts with it.

### 1. One result contract for every harness

Add four blocks to the JSON each harness writes. Nothing existing is renamed, so old results and the old report keep working.

| Block | Contents |
|---|---|
| `settings` | Every effective setting by name: store, payload bytes, callers, producer slots, worker slots, bulk chunk, claim and finish options, grouping on/off, lease, connection pool size, GC mode. For SQLite: `journal_mode`, `synchronous`, `wal_autocheckpoint` and SQLite version, read from both the producer and the worker connection. |
| `outcome` | `offered`, `accepted`, `refused`; then, counted **in the store** after the run: `readyBeforeStart` (drain), `succeeded`, `failed`, `unfinished`; plus `handlerReturns`, `uniqueHandlerReturns`, `duplicates`. |
| `clocks` | Seconds and jobs/s from worker start and from first claim (drain); for steady: completed during the offer window, and time to clear what was left. |
| `progress` | Drain only: settled count once per second, so an incomplete round still gives time to 50%, 95%, 99%. |

A round is **complete** when `accepted == succeeded + failed` and `unfinished == 0` (for add-only tests: accepted equals rows in the store). Throughput is computed from the store-confirmed count. An incomplete round is published as incomplete, never as a rate.

Each harness also prints a marker line to stdout at `seed-start`, `seed-end`, `timed-start` and `timed-end`.

Changes by harness:

- **Runwick** ([BenchMode.cs](../../tests/Runwick.Workloads/BenchMode.cs)): emit the four blocks and markers. Generalise the SQLite-only `UnfinishedAsync` (line 1235) into a per-store count by state. Verify the seeded backlog before starting the worker. No change to how anything is timed.
- **Wolverine** ([Program.cs](../../eng/bench-compare/wolverine/Program.cs)): move host construction out of the clock; count incoming, handled and failed rows in its tables; send with the existing inline setting.
- **BullMQ .NET** ([Program.cs](../../eng/bench-compare/bullmq-dotnet/Program.cs)): attach Completed and Failed listeners before the worker starts; count waiting, active, failed at cutoff; add `--batch`.
- **BullMQ Node, River, pg-boss, Solid Queue**: markers, store counts, and `--batch` for River and Solid Queue.

Done when: every harness passes a `--scale 10` smoke run with a complete round, and a deliberately failing job shows up as `failed: 1`, not as a hang.

### 2. Driver: same host, phase-aware, self-describing

All in [run.py](../../eng/bench-compare/ci/run.py):

- **Several libraries in one invocation** (`--systems a,b,c`), order reversed on even rounds. Before each library, recreate its store container from scratch using one pinned command (image digest, `--cpus`, the settings now written inline in `bench.yml`). Move that command into `run.py` so local and CI runs cannot drift.
- **Read at the markers.** Read harness stdout line by line; at each marker take an exact reading of the app process tree, the store container, and store counters (PostgreSQL: WAL position, WAL bytes, full-page images, commits. Redis: AOF size, commands processed. SQLite: WAL file size). Keep one long-lived `psql` / `redis-cli` pipe open so a reading takes about a millisecond. `cpuMsPerJob`, wire bytes and disk bytes per job are then computed for the timed phase only; the old whole-run figures stay alongside for continuity.
- **Manifest.** One `manifest.json` per run: Runwick commit and a clean-tree flag, the `src == v0.1.5` check result, every library's locked version, store image digests, the store's effective durability settings read back from the server, CPU quota, filesystem and device of the data directory, kernel, .NET GC settings, and the exact command line of every round.
- **New rows.** `enqueue-c64`; `bulk-100` (equal chunk for every library) next to `bulk-native`; and a second Runwick Redis arm, `runwick-redis-tuned` (`--buffered-finish --claim-ahead 16 --claims-in-flight 2`), kept separate from the default arm.
- **One trace round** per Runwick cell with `--trace`, stored separately and never mixed into timing medians. This supplies the preparation, queue-wait and store-call split the Stage 1 table asks for without slowing the timed rounds.

Done when: a local smoke run of one store family produces a manifest, phase-specific figures, and identical results layout to CI.

### 3. Floor measurements for the cost budgets

The plan asks what the cheapest possible durable write costs on the same disk, before anyone optimises a serializer. `floor` mode already times a minimal PostgreSQL insert and commit. Add the two missing ones to [FloorMode.cs](../../tests/Runwick.Workloads/FloorMode.cs): a single `XADD` against the AOF-always Redis, and a one-row insert with `synchronous=FULL` commit on SQLite, each from 1 and 16 callers. Run them in the same job as the baseline so they share the disk.

Done when: the report has a "floor" line per store in microseconds per write, next to the 396 µs / 246 µs / 96 µs budgets.

### 4. Report

In [report.py](../../eng/bench-compare/ci/report.py):

- Show every round, the median and the min–max spread, not just the median.
- Show incomplete rounds in their own column with how far they got, instead of dropping them.
- Add the outcome columns (accepted, succeeded, failed, unfinished, duplicates) and both drain clocks.
- Generate the **baseline → target** table from the run itself: target = 90% of the fastest same-store library on the same host, naming the leader per cell.

Done when: feeding it the November artifacts still works, and feeding it a new run produces the target table without hand editing.

### 5. Workflow

Add a `same-host` mode to [bench.yml](../../.github/workflows/bench.yml): three jobs (PostgreSQL, Redis, SQLite), each building every library for that store and calling `run.py --systems ...` once. Raise that job's timeout, because Wolverine PostgreSQL needs about 260 s per drain. The existing one-runner-per-library mode stays untouched.

### 6. Run the baseline and publish

1. Smoke: `scale 10`, 1 round, all three jobs. Fix whatever does not reconcile.
2. Baseline: full scale, **5 rounds**, alternating order, plus the trace round and the floor measurements.
3. Re-run any cell whose spread is wider than 15% or that has an incomplete Runwick round.
4. Publish `docs/reports/queue-baseline-v0.1.5.md` with the manifest and raw rounds under `docs/reports/evidence/baseline-v0.1.5/`; update the targets table in the throughput plan with the same-host numbers; update the docs site results page and append the run to the bench dashboard.

## Order and who does what

| Step | Depends on | Notes |
|---|---|---|
| 0 Sync | – | |
| 1 Result contract | 0 | Runwick harness first, since it defines the field names the others copy. The six competitor harnesses are independent of each other. |
| 2 Driver | 1 (marker names) | |
| 3 Floor | 0 | Independent of 1 and 2. |
| 4 Report | 1 (field names) | |
| 5 Workflow | 2 | |
| 6 Run and publish | all | |

Per the standing arrangement: Opus writes the changes, Sonnet runs the smoke and baseline runs and waits for them, Haiku handles the small harness edits.

## Left out of Stage 1 on purpose

- 64 KiB and near-limit payloads, priorities, delayed jobs, duplicate keys, mixed contracts, the retained million-row fixture, a second host, and non-loopback network. The plan says to extend only promising arms, so these wait until a Stage 3 or 4 candidate exists.
- Preparation-versus-store timers inside the dispatch path. That is Stage 2's experiment; Stage 1 only records what `--trace` already gives.
- Any gate-checking tool for candidate changes. `ab.sh` plus the paired rules in the throughput plan are enough until there is a candidate.
- The CPU-heavy workload. Unchanged and not rerun.
- Horizon and Celery.

## Exit checklist

- [ ] `src/` still identical to `v0.1.5`, recorded in the manifest.
- [ ] Every baseline cell has 5 rounds on one host per store, each marked complete or incomplete from store counts.
- [ ] Phase-specific CPU, wire and disk bytes per job for every cell.
- [ ] SQLite durability settings recorded for Runwick, Wolverine and Solid Queue, and the equal-size `bulk-100` row exists.
- [ ] Floor cost per store published next to its budget.
- [ ] Baseline report, manifest and raw rounds committed; throughput plan targets replaced with same-host figures.
