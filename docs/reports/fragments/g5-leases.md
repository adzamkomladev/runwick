# G5 fewer lease renewals: measurement modes

The fewer-renewals profile (`WorkerOptions.UseFewerLeaseRenewals()`: a 5 minute lease renewed about every 90 s) only had a
fake-time test. The `bench` command in `tests/Runwick.Workloads` can now measure it on a real store in three ways. Every flag is
opt-in; a bench run without them behaves and reports exactly as before.

## What was built

1. **Lease settings and a cost report.** `--fewer-lease-renewals` turns the profile on. `--lease-seconds n` and
   `--renew-seconds m` set the lease and renewal interval by hand (jitter is kept at no more than a fifth of the interval). Any of
   these flags, or `--lease-report` for the default profile, adds a `lease` block to the result JSON.
2. **Crash-detection latency.** `bench --lease-crash` starts a worker process that takes one long job per slot, kills it with
   SIGKILL as soon as every slot is busy, and starts a second worker on the same store at once. It reports how long each of the
   killed worker's jobs took to start on the second worker. Works on SQLite, PostgreSQL and Redis.
3. **Store outage.** In steady mode, `--outage-container NAME --outage-at s --outage-seconds d` runs `docker pause NAME` s seconds
   into the run and `docker unpause NAME` d seconds later, then reports what happened to the jobs that were running. PostgreSQL and
   Redis only (SQLite has no container).

## The new output fields

`lease` block (sweep and outage runs):

| Field | Meaning |
|---|---|
| `leaseSeconds`, `renewSeconds`, `renewJitterSeconds` | The lease settings the worker actually ran with. |
| `runSeconds` | Time from just before the worker started to the end of the measurement. |
| `jobsFinished`, `completionsPerSec` | Load jobs whose finish the store confirmed, and that number per second of `runSeconds`. |
| `renewsSent` | Lease renewals the worker sent (one per running job per renewal, whether sent alone or in a batch). |
| `renewManyCalls` | How many of those went out as one batched RenewMany store call. |
| `renewsPerJob`, `renewsPerSec` | `renewsSent` per finished job and per second. |
| `lostByCause` | Leases the worker lost, by cause (for example `SafetyDeadline`, `StaleLease`). Empty means none was lost. |
| `storeCounters` | What the store itself counted during the run (after minus before). Redis: `cpuSeconds` (server CPU), `commandCalls`, `commandUsec` and `calls:<command>`. PostgreSQL: `transactions` and `rowsWritten`; with `--trace` also `functionCalls:<fn>` and `functionMs:<fn>` per Runwick SQL function (PostgreSQL does not report its own CPU). SQLite: `null`; the existing `sqlite.writeCommits` field gives the write count. |

`outage` block (outage runs):

| Field | Meaning |
|---|---|
| `pauseExit`, `unpauseExit`, `pausedForMs` | Exit codes of the docker commands and how long the store was actually paused. |
| `runningAtPause` | Load jobs whose handler was running when the store was paused. |
| `keptLeaseAndFinishedOnce` | Of those, how many kept their lease, ran once and finished. |
| `lostLease` | Of those, how many lost their lease (the worker gave the job up; it runs again later). |
| `ranTwice` | Of those, how many had their handler started more than once. |
| `finishedAfterStoreBack` | Of those, how many finished after the store came back. |
| `notFinished` | Of those, how many had not finished when the run ended. |
| `allJobsRunMoreThanOnce` | Every load job of the run whose handler started more than once. |

Error codes during the outage are in the existing fields: `failedDispatchByCode` (refused dispatch calls), `lease.lostByCause`,
and `diagnostics`.

`--lease-crash` result:

| Field | Meaning |
|---|---|
| `leaseSeconds` | The lease the run used (300 with `--fewer-lease-renewals`). |
| `jobsAccepted` | Jobs the first worker dispatched. |
| `orphans` | Jobs the first worker was running when it was killed. |
| `orphansPickedUp`, `orphansNotPickedUp` | How many of those the second worker started, and how many it never started before `--detect-seconds` (default twice the lease plus 60 s). |
| `detectionMinMs`, `detectionMs` | Time from the kill to the second worker starting each orphan (minimum, and count/p50/p95/p99/max). Expect about the lease length plus up to one recovery scan (5 s) and the second worker's startup. The kill comes right after the jobs started, so this is the upper end: a worker that dies just before a renewal frees its jobs one renewal interval sooner. |
| `secondWorker` | The second worker's own bench result. |
| `directory` | Ledgers, child logs and (SQLite) the database file; kept for inspection. |

## Full-run commands

Run each from `tests/Runwick.Workloads` after `dotnet build -c Release`, one at a time, under the bench lock. `B` stands for
`flock /tmp/runwick-bench.lock dotnet bin/Release/net10.0/Runwick.Workloads.dll bench`. PostgreSQL uses the default bench
container (`runwick-bench-pg` on port 55499). Redis needs a throwaway instance started with `--appendonly yes --appendfsync always
--aof-load-truncated no --maxmemory-policy noeviction --save ''`; `$REDIS` is its `host:port` and `$REDIS_CONTAINER` its container
name. Put the store flags (`--store sqlite`, `--store postgres`, or `--store redis --redis $REDIS`) where `STORE` is written.

**1. Timeout sweep** (three arms per store, about 7 minutes each, about 1 hour for all nine):

```
$B STORE --mode steady --workers 16 --job-ms 120000 --rate 0.12 --seconds 300 --lease-report --out sweep-STORE-default.json
$B STORE --mode steady --workers 16 --job-ms 120000 --rate 0.12 --seconds 300 --lease-seconds 120 --renew-seconds 30 --out sweep-STORE-120.json
$B STORE --mode steady --workers 16 --job-ms 120000 --rate 0.12 --seconds 300 --fewer-lease-renewals --out sweep-STORE-fewer.json
```

Add `--trace` on PostgreSQL to get per-function call counts in `storeCounters`. Compare `lease.renewsPerSec`, `lease.renewsPerJob`,
`lease.completionsPerSec` and `lease.storeCounters` across the arms. Jobs still running at the end are cancelled by the graceful stop.

**2. Crash-detection latency** (three arms per store; about 40 s, 75 s and 5.5 minutes; about 22 minutes for all nine):

```
$B STORE --lease-crash --workers 16 --lease-seconds 30 --renew-seconds 5 --out crash-STORE-30.json
$B STORE --lease-crash --workers 16 --out crash-STORE-default.json
$B STORE --lease-crash --workers 16 --fewer-lease-renewals --out crash-STORE-fewer.json
```

**3. Pause and outage** (PostgreSQL and Redis; four arms each, an outage below and above the lease for each profile; about
5 minutes each except the 360 s arm at about 8 minutes; about 45 minutes for all eight):

```
$B STORE --mode steady --workers 16 --job-ms 20000 --rate 0.5 --seconds 240 --outage-container C --outage-at 60 --outage-seconds 20  --out outage-STORE-default-20.json
$B STORE --mode steady --workers 16 --job-ms 20000 --rate 0.5 --seconds 240 --outage-container C --outage-at 60 --outage-seconds 90  --out outage-STORE-default-90.json
$B STORE --mode steady --workers 16 --job-ms 20000 --rate 0.5 --seconds 240 --outage-container C --outage-at 60 --outage-seconds 120 --fewer-lease-renewals --out outage-STORE-fewer-120.json
$B STORE --mode steady --workers 16 --job-ms 20000 --rate 0.5 --seconds 240 --outage-container C --outage-at 60 --outage-seconds 360 --fewer-lease-renewals --out outage-STORE-fewer-360.json
```

`C` is `runwick-bench-pg` for PostgreSQL and `$REDIS_CONTAINER` for Redis. The outage pauses the whole container, so nothing else
may use it during the run. If a run is killed while the store is paused, run `docker unpause C` by hand.

## Not built

- Store CPU for PostgreSQL and SQLite: neither reports it; PostgreSQL gives transactions, rows and (with `--trace`) function calls.
- The crash run kills right after the jobs start; there is no flag to kill at a chosen point in the renewal cycle.
- No SQLite outage mode (an in-process store has nothing to pause).
