

## Delayed jobs from staging, outbox, management and schedules now wake idle workers

Written 2026-11-22. Follows "PostgreSQL and SQLite: delayed jobs picked up seconds late (fixed)" in perf-round-2.md, which fixed direct `Dispatch` and `DispatchMany` only. Branch: `claude/frosty-wing-ac9ea5`, on top of main `1d45bc0`.

### Short answer

A delayed job (one sent with `NotBefore`) that becomes the earliest waiting job of its queue and priority now wakes idle workers on every path that creates jobs, not only direct dispatch. Before, a job created by staging inside the caller's transaction, by outbox delivery or by management dispatch sent no wake, so a worker that had been idle for a while found it only when its back-off poll fired: up to 10 seconds (plus or minus 20 %) late on PostgreSQL, up to 2 seconds on SQLite.

The rule is the same as for direct dispatch: wake only when the new job is its lane's earliest not-yet-due job, only after the transaction has committed, never for a resent (replayed) command. Jobs queued behind an earlier delayed job send nothing, so a steady stream of delayed jobs costs almost no extra wakes.

| Path | PostgreSQL | SQLite | Redis |
| --- | --- | --- | --- |
| Staging (caller's transaction) | fixed: new SQL function `dispatch_v5`, migration 0027 | fixed | no staging on Redis |
| Outbox delivery | fixed (single and grouped) | fixed | already woke |
| Management dispatch | fixed | fixed | already woke |
| Schedule materialization | nothing to fix (never delayed) | nothing to fix for delayed; due jobs now wake too | already woke |

### What changed

**Schedules never create delayed jobs.** A schedule template refuses `NotBefore`, and an occurrence is only materialized once it is due, so a scheduled job is always due when it is created. Its queue was already woken on PostgreSQL and Redis. No delayed-wake code was needed there.

**Redis: no code change.** Every Redis script that creates jobs ends in the same projection step (`U.project` in `prelude.lua`), and that step already calls `U.wake`, which publishes a wake for a due job or a lane's new earliest delayed job. New contract tests confirm this for outbox delivery, management dispatch and schedule materialization. This keeps the branch clear of the Redis script work going on elsewhere.

**PostgreSQL, staging.** A staged dispatch runs in the caller's own transaction, and the client has no way to act after that transaction commits. So the notification has to be sent from SQL, inside the transaction: PostgreSQL holds a `NOTIFY` until `COMMIT` and drops it on rollback. Migration `0027_staged_delayed_wake` (schema revision 27) adds `dispatch_v5`, which runs `dispatch_v3` unchanged and then, for each created job not yet due, sends the notification if no other not-yet-due job of its lane comes before it in claim order. That check is the one the client-side wake pump already uses (one primary-key and one ready-index lookup per job). Staged calls now use version 5. `dispatch_v3` and `rw_dispatch_core_v3` are untouched, so binaries at earlier revisions keep working against them.

I chose a thin wrapper over a new copy of `rw_dispatch_core_v3`. A copy would also need new copies of every function that calls it (`dispatch_v3`, `receive_outbox_v3`, `receive_outbox_group_v1`, and the schedule functions), several hundred lines of duplicated SQL, while only staging needs SQL at all.

**PostgreSQL, outbox and management.** These run in transactions the driver owns, so they use the same post-commit wake pump as direct dispatch, with no SQL change. After a committed first delivery (single or grouped) or management dispatch that created a delayed job, the client hands the job to the pump. A redelivery answered from the outbox mapping, and a replay, hand over nothing. For management dispatch this goes through a small "after commit" list on the driver's transaction object, which the runner runs only after `COMMIT` succeeds.

**SQLite, outbox, management and schedules.** These paths sent no in-process wake at all, not even for jobs due now. They now send the same hint as direct dispatch, after commit, never on replay. So on SQLite, jobs due now from these paths also wake workers in the same process at once (before: up to the 2 s back-off).

**SQLite, staging.** The .NET SQLite library has no commit callback. For a staged delayed job the store waits for the caller's transaction to end (it checks every 20 ms, for at most one minute), then runs the usual "is it the lane's earliest" read. A rolled-back job is not in the database, so it wakes nothing. Staged jobs that are due now still send no hint (unchanged; outside this task).

### Tests

New tests, each checking: a wake for the lane's first delayed job, none for a later one, a wake again for an earlier one, and none for a replay.

- Contract suite, run on PostgreSQL, SQLite and Redis:
  - `OutboxContractTests.A_delivered_delayed_job_wakes_its_queue_only_as_its_lanes_earliest_and_never_on_replay` (also checks that a redelivery does not wake, and on PostgreSQL a grouped delivery wakes once for its earliest item).
  - `ManagementContractTests.A_delayed_management_dispatch_wakes_its_queue_only_as_its_lanes_earliest_and_never_on_replay`.
  - `ScheduleContractTests.A_materialized_occurrence_wakes_its_queue_once_and_never_on_replay` (the job is due, so this checks the one wake and no wake on replay).
- `Runwick.Postgres.Tests.WakeNotificationTests.A_staged_delayed_job_notifies_on_commit_only_as_its_lanes_earliest_and_never_on_replay_or_rollback`: nothing is sent before the caller commits, nothing after a rollback; a batch wakes once for its earliest item.
- `Runwick.Sqlite.Tests.SqliteStagingTests.A_staged_delayed_job_wakes_local_workers_after_commit_only_as_its_lanes_earliest_and_never_on_replay_or_rollback`: the same on SQLite.
- `MigrationTests` updated for revision 27.

Results so far (2026-11-22 and 2026-11-23, shared host):

- All the new tests pass: the contract wake cases (9 of 9: outbox, management and schedule on PostgreSQL, SQLite and Redis), the SQLite staged case, and the PostgreSQL `WakeNotificationTests` class (6 of 6, including the new staged case).
- The new PostgreSQL staged test fails on a control build with the PostgreSQL change switched off, so it does catch the gap.
- Full Core suite: 455 passed, 0 failed.
- Full Contract, SQLite, Redis and PostgreSQL suites: not yet run (the run was stopped when the work was handed to another session).

### Quick pickup check

PostgreSQL only, on the shared host (not the reference host), holding `/tmp/runwick-bench.lock`, 2026-11-23 after a reboot (load average 11 to 85 during the runs, mostly from builds). A temporary test (not committed) started one worker with production timings, let it sit idle for 14 seconds so its back-off reached the 10-second cap, then created one job due 3 seconds later, first by staging in a committed transaction and then by management dispatch, three rounds each. It recorded how long after the due time the handler started.

| Arm | Staged: started after due | Management: started after due |
| --- | --- | --- |
| This branch | 138, 39, 55 ms | 138, 83, 30 ms |
| Control (same build with the two PostgreSQL changes switched off: staging back on `dispatch_v3`, management not handed to the pump) | 7.3, 6.8, 6.9 s | 5.1, 6.4, 5.1 s |

The control shows the old behaviour: the job waits for the worker's next back-off poll. With the change the worker is woken when the job is committed, reads the due time from its readiness check and sleeps until then. On the same control build the new staged wake test fails, as expected.

An earlier run on 2026-11-22, on a host at load 30 to 100, gave the same picture on the branch (19 to 143 ms) except one management round at 19.8 s, longer than the 12-second worst case of the old back-off poll, during a load spike to 102; I read that as the host starving the process, not a missing wake. Its control arm was lost to the host reboot. SQLite and Redis were not timed: Redis needed no change, and on SQLite the old worst case is the 2-second back-off and host fsync stalls dominate its tails (see the 2026-11-19 report).

### Not done

- Retry back-off still sends no store wake. A running worker's own finish wakes its pool, so its retries start on time (see the 2026-11-19 report); this is unchanged.
- SQLite staged jobs that are due now still send no in-process hint.
- The plan line in `docs/plans/perf-plan-2-remaining.md` ("still not woken: ...") is not edited here, to avoid merge conflicts with the other session editing that line; it can be updated when this branch is merged.
