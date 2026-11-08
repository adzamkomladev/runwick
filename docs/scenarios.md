# Scenario catalogue (SP-027)

Each scenario below is a real program run, not a unit test: separate operating-system processes start against a real PostgreSQL, a real SQLite file or a real Redis, do something an operator would do, and a check at the end says PASS or FAIL. Every scenario has an executable in [samples/Runwick.Scenarios](../samples/Runwick.Scenarios) and a line in the runner [samples/run-scenarios.sh](../samples/run-scenarios.sh). Results of the last run are in [reports/sp-027.md](reports/sp-027.md).

```
samples/run-scenarios.sh            # all 33
samples/run-scenarios.sh SCN-02     # one
dotnet samples/Runwick.Scenarios/bin/Release/net10.0/Runwick.Scenarios.dll list
```

What it needs: PostgreSQL (each scenario creates and drops its own database), Redis with the AOF-always profile (each scenario uses and flushes one numbered database), nothing for SQLite (a temporary file). Addresses come from `RUNWICK_TEST_PG` and `RUNWICK_TEST_REDIS`.

How a scenario sees the outcome: every job the scenarios run leaves one small file ("which process, when it started, when it ended") in a folder. Counting the files proves the work ran, naming the processes proves who ran it, and the start and end times prove how many ran at once. This works the same on every store, so a store cannot "pass" by being checked more gently.

## Deployment modes, three stores (SCN-01 to SCN-15)

The five modes of [spec 00 §5](spec/00-overview.md). The same program is used for every mode; only the roles differ ([spec 00 §3](spec/00-overview.md)). 30 jobs are produced in each scenario; the pass rule is that exactly 30 distinct jobs ran, and that no producer-only process ran any.

| Mode | What runs | PostgreSQL | SQLite | Redis |
|---|---|---|---|---|
| 1. One application, embedded workers | one process that produces and works | SCN-01 | SCN-06 | SCN-11 |
| 2. One application, external worker | an application that only produces, plus a separate worker process | SCN-02 | SCN-07 | SCN-12 |
| 3. Several application replicas, each embedded | three processes, each producing and working | SCN-03 | SCN-08 | SCN-13 |
| 4. Application replicas with several external workers | two producer-only web processes, two worker-only processes | SCN-04 | SCN-09 | SCN-14 |
| 5. Embedded and external workers together | two replicas that produce and work, plus two worker-only processes | SCN-05 | SCN-10 | SCN-15 |

SQLite is same-host only ([spec 20 §1](spec/20-sqlite.md)): all five modes apply, provided every process runs on the one machine. The scenarios do exactly that (several processes on one file). A SQLite deployment across hosts is not a scenario because it is not supported.

## Who may use the management API and the dashboard (SCN-16 to SCN-18)

[Spec 16 §5](spec/16-management.md) and [spec 21 §3.2](spec/21-dashboard.md). Authentication does not depend on the store, so these run on PostgreSQL only.

- **SCN-16, Basic with antiforgery.** No credentials, a wrong password, and a right password over plain HTTP are all 401. The right password over HTTPS (the scenario puts a trusted proxy in front and sends `X-Forwarded-Proto`) is 200. A change without the antiforgery token is 400, with it 200, and a viewer with a valid token is still 403.
- **SCN-17, bearer token for machine clients.** The host's own bearer scheme (a stand-in token handler here) on a machine-client group: no token and the wrong scheme are 401; a viewer can read but gets 403 on a change; an operator can change without an antiforgery token; a grant limited to a different queue sees no queues at all.
- **SCN-18, dashboard cookie login.** The existing `Runwick.Dashboard.Host` sample runs as its own process: signed out, the dashboard page sends the browser to the login page; signed in as admin it returns the page.

## Schedules (SCN-19 to SCN-21)

A schedule that fires every 2 seconds, declared by two scheduler processes at once, with a third process doing the work. Pass: between 3 and 10 jobs in 14 seconds (the two schedulers must not double the fires) and all of them ran on the worker process. Setup step: the scenario raises the `scheduler` capability with the operator call (`runwick store activate-capability`, library form `ActivateCapabilityAsync`) after starting the application once.

| PostgreSQL | SQLite | Redis |
|---|---|---|
| SCN-19 | SCN-20 | SCN-21 |

## Capacity (SCN-22 to SCN-24)

Two parts per scenario. First: a queue limit of 3 across two worker processes that could each run 4 at once; pass is that never more than 3 ran at the same instant, and at least 2 did. Second: one worker with `AutoConcurrency(1, 6)` under a backlog of 80 half-second jobs; pass is that it grew beyond 1 and never beyond 6 (the autoscaler needs three growth samples and waits 30 seconds between changes, so the run is slow on purpose).

| PostgreSQL | SQLite | Redis |
|---|---|---|
| SCN-22 | SCN-23 | SCN-24 |

## Drain and reopen (SCN-25 to SCN-27)

[Spec 23](spec/23-upgrades.md) and [the upgrade runbook](runbooks/upgrade.md) steps 2 and 5, using the product's own admission call (the same one behind `runwick store admission`). A worker runs; work is done; the application is drained; a dispatch is refused with `AdmissionClosedException`; a **new process started while drained exits with code 75 and a message that names the fix** (the SP-027 change, not a crash); admission is reopened; the original worker serves new work again; a new process then starts normally.

| PostgreSQL | SQLite | Redis |
|---|---|---|
| SCN-25 | SCN-26 | SCN-27 |

## Store outage and recovery (SCN-28 to SCN-30)

Work runs; then the store becomes unreachable for about 6 seconds. PostgreSQL and Redis: a small relay in front of the store cuts every connection and refuses new ones. SQLite: another connection holds the write lock. Pass: a dispatch during the outage fails with a Runwick error (it does not hang for more than 30 seconds); the worker process keeps running; after the outage the same producer dispatches again without a restart and the worker runs the new work.

| PostgreSQL | SQLite | Redis |
|---|---|---|
| SCN-28 | SCN-29 | SCN-30 |

## Outbox (SCN-31 to SCN-33)

[Spec 14](spec/14-outbox.md). Ten jobs are staged inside the application's own database transaction together with a business row; one more is staged and rolled back. A relay delivers them to a Runwick destination and a worker runs them. Pass: exactly ten jobs ran (the rolled-back one never did). Sources are PostgreSQL and SQLite (the two source adapters that exist); destinations are all three stores.

| Source | Destination | Scenario |
|---|---|---|
| PostgreSQL | PostgreSQL | SCN-31 |
| SQLite | SQLite | SCN-32 |
| SQLite | Redis | SCN-33 |

## Not in this catalogue

- The CLI's `store admission` command is not driven by a scenario (the scenarios call the same library function; the CLI has its own tests, CLI-UPG). Redis has no CLI command at all.
- Real browser clicking of the dashboard is SP-024's job; SCN-18 checks only who is let in.
- Performance numbers and long soaks are the perf plan's job, not this catalogue's.
