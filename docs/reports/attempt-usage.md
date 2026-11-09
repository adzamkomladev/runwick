# Attempt usage report: CPU time and memory per attempt

Date: 2026-11-09. Branch `claude/attempt-usage`, merged into `main`.

## What was built

The worker now measures each attempt and stores the figures with the attempt when its Finish is applied. The job page, `GetJob`, the HTTP API and the CLI's JSON show them. Attempts finished before this change, or by a worker with the option off, read "not recorded".

| Figure | What it means |
|---|---|
| Process CPU time | CPU time the whole worker process used between the attempt's start and end, in microseconds. Exact. |
| Process allocations | Bytes the whole process allocated on the managed heap in that window. Exact. |
| Working set at start, end and peak | The process's resident memory. Read by one shared timer every 250 ms while any attempt runs, because each read costs about 0.1 ms on Linux. Start, end and peak are those samples. |
| Overlapping attempts | How many other attempts ran in the same process during the window. 0 means the figures belong to this attempt alone, apart from the worker's own background work. |

**Why process-wide.** .NET has no CPU or allocation counter that follows one async job as it moves between threads. Every figure is labelled as the whole process's. The dashboard card says so and shows "alone" or "with N others" for each attempt.

## Where it is stored

| Store | Change | Upgrade |
|---|---|---|
| PostgreSQL | Schema revision 18 (`0018_attempt_usage`): a nullable `usage jsonb` column on `job_attempts`. New `finish_v3` and `finish_many_v3` call the released `finish_v2` and `finish_many_v2` unchanged, then store the usage of each item that applied. Released functions are not edited. | Adding a nullable column rewrites nothing. |
| SQLite | Revision 2 (`0002_attempt_usage`): a nullable `usage` TEXT column, checked to be a JSON object of at most 1 KiB. The migrator now applies migrations in order and records each in the ledger. | As for PostgreSQL. |
| Redis | One more field in the attempt hash, written by the `finish_job` script. | No layout revision: a missing field already reads as null. |

The stored value is one small JSON object: `{"v":1,"scope":"process","cpuUs","allocBytes","wsStart","wsEnd","wsPeak","overlap"}`. A missing value, an unknown version or a damaged value reads as "not recorded" and never fails the read.

The usage is not part of any receipt. A replayed Finish returns the same reply and never changes figures already stored. The command fingerprint gains a `usage` key only when usage is present, so fingerprints of Finishes without usage are unchanged.

**Upgrade note.** v1 runs one binary against exactly one store revision (spec 23). A binary from before this change is refused by a store at revision 18 (PostgreSQL) or 2 (SQLite) by design. Upgrade the binaries with the migration, as for any revision.

## The option

`WorkerOptions.RecordResourceUsage`, **on by default**. With it off nothing is read and attempts store no usage.

## Overhead

Measured on this development host (not the reference host). PostgreSQL ran in the `runwick-pg` container. Each run was a `Runwick.Workloads bench`; `--no-usage` turns recording off.

**Drain:** 20 000 jobs, 16 workers, on and off alternating. Figures are jobs completed per second.

| Pair | On | Off |
|---|---|---|
| 1 | 103.5 | 100.2 |
| 2 | 104.7 | 86.4 |
| 3 | 88.2 | 101.9 |
| Median | **103.5** | **100.2** |

**Steady:** 200 jobs/s offered for 60 s, one run each. The host completed only about 48 jobs/s, so this run is saturated rather than a clean latency test.

| | On | Off |
|---|---|---|
| Jobs/s | 47.4 | 49.1 |
| p50 | 58.9 ms | 59.8 ms |
| p95 | 166.8 ms | 158.0 ms |
| p99 | 305.1 ms | 469.1 ms |

**Reading.** There is no difference between on and off beyond the noise. Runs vary by about 15%, and which side wins changes from pair to pair. The per-attempt cost measured in isolation is about 12 µs of counter reads. A working-set read every 250 ms adds about 0.1 ms per second. Both are small next to a store round trip. So the default stays on.

## Tests

| Case | Where | Checks |
|---|---|---|
| USG-01 | Contract tests on all three stores | A replayed Finish keeps the first figures and the same reply. |
| USG-02 | Contract tests on all three stores | FinishMany stores usage per item, only for items that applied. |
| USG-03 | Worker contract test | A handler that burns about 300 ms of CPU and allocates 64 MiB records at least 250 ms of CPU, at least 64 MiB allocated and 0 overlapping attempts. An idle handler records less than half that CPU. |
| USG-04 | Worker contract test | With the option off, nothing is recorded. |
| USG-05 | SQLite | A revision 1 file migrates to 2, and old attempts read "not recorded". |
| Migration | PostgreSQL `MigrationTests`, SQLite and CLI tests | Revision 18 and revision 2 apply, including each kill-phase case of 0018. |
| Dashboard | e2e step | The job page shows CPU and working-set figures and one row per attempt. |

## Test runs on the final build

| Suite | Result |
|---|---|
| PostgreSQL | 368 passed, 2 skipped, 3 failed. The 3 failures restart the store and need a Docker-managed server. They fail the same way on `main` when pointed at an external server. |
| Contract (usage, worker, FinishMany, core, management and upgrade filters) | 89 passed |
| SQLite | 51 passed |
| CLI | 20 passed |
| AspNetCore | 19 passed |
| Core `PackageGraphTests` | 1 failed, and it fails on `main` too. The committed `Runwick.Cli` lock file has a `win-x64` section without Cronos (added in 0e39863). A local restore rewrites the file and hides the failure. Not caused by this change. |

Two tests were fixed along the way:
- The SQLite upgrade case still restored revision 1.
- The events case read the events page once. That page only shows events below the server-wide snapshot horizon (08 §7.2), so a concurrent transaction elsewhere on the shared test server could hide the new event. The case now waits until the event is visible.
