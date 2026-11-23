# Queue library benchmark on GitHub Actions

Run it from **Actions → bench → Run workflow** (manual only). Inputs: `rounds` (default 3; the report shows the median)
and `scale` (default 1; use 10 for a quick smoke run that divides job counts and durations by 10).

## What runs

Each of the 28 combinations below (14 libraries x 2 workloads) gets its own fresh `ubuntu-24.04` runner, so they run at
the same time and never share a CPU (GitHub starts 20 at once on a free plan; the rest follow). A last job collects everything.

| Library | Store | Workers |
|---|---|---|
| Runwick PostgreSQL / SQLite / Redis | PostgreSQL 18 / local file / Redis 8.2 | one .NET process |
| Wolverine PostgreSQL / SQLite / Redis | its own queue transport on each | one .NET process, one listener |
| BullMQ .NET (official port) | PostgreSQL 18 / Redis 8.2 | one .NET process, one Worker |
| River | PostgreSQL 18 | one Go process |
| pg-boss | PostgreSQL 18 | one Node process (batch fetch) |
| Solid Queue (Rails 8) | local SQLite file (Rails 8's default) | `bin/jobs`: min(slots, cores) processes × threads |
| BullMQ | Redis 8.2 | one Node process |
| Laravel Horizon | Redis 8.2 | one PHP process per slot |
| Celery | Redis 8.2 | prefork pool, one process per slot |

Every .NET contender runs with server GC (`DOTNET_gcServer=1`), as a production worker service would.

Wolverine sends to Redis with `SendInline()`, so an enqueue counts only once Redis holds the message (its default is an
in-memory buffer); its PostgreSQL and SQLite queues are always durable. Wolverine and Celery have no bulk-enqueue call.

Every Redis library runs against the same server settings: AOF with `appendfsync always`, no RDB snapshots,
`noeviction` (Runwick's required profile). Everything else is each library's default.

Two workloads, same tests for each library:

- **normal**: a 1 KiB job that does nothing. Tests: push 1 at a time with 1 and 16 callers, bulk push 20,000,
  process a 20,000-job backlog with 16 and 64 worker slots, and steady arrivals at 100 and 500 jobs/s for 30 s.
- **cpu**: the same job doing 50,000 rounds of SHA-256 over a 1 KiB buffer (each digest written back into the first
  32 bytes; after 50,000 rounds the first 32 bytes are `164f3d9c…32ff6f`). That is about 0.1–0.5 s of one core depending
  on the language. Tests: process a 400-job backlog with 16 slots, and 4 jobs/s arriving for 60 s.
  Node libraries run every job on one core, so they fall behind here; that is how they behave by default.

## What is measured

The library's own harness (`../<library>/bench.*`) reports throughput and latency. `run.py` wraps every run and samples,
every 0.5 s:

- **App**: every process the run started (producers, workers, driver): CPU (exact total from the kernel, plus average
  and peak %), memory (PSS, so forked workers are not double counted), disk bytes.
- **Store**: the PostgreSQL or Redis container's cgroup: CPU, memory, disk bytes, and network bytes in and out
  (the bytes each library puts on the wire).

From those it derives **CPU ms per job** (app + store) and **bytes per job**. Every result also records the exact
library version that ran (for Runwick, `git describe` of the commit).

## Output

The run summary page shows every table and a bar chart per test. The `bench-report` artifact holds `report.html`
(open it in a browser: bar charts for every metric), `report.md` and `results.json`. Each `bench-<library>-<workload>`
artifact holds the raw JSON and logs of every run.

Runners can land on different CPU models; the Hosts table says which each job got. Compare within one run.

## Running one locally

```bash
python3 eng/bench-compare/ci/run.py --system river --workload cpu --rounds 1 --scale 10 --out /tmp/bench
python3 eng/bench-compare/ci/report.py /tmp/bench /tmp/bench/report
```

It expects PostgreSQL on 127.0.0.1:55499 (password `pg`) and Redis on 127.0.0.1:56392, in containers named
`bench-pg` and `bench-redis` for the store figures, and the library built as in `.github/workflows/bench.yml`.
