## PostgreSQL grouped producer admission, redesigned (B1)

Written 2026-11-23. A fragment for `perf-round-2.md`; the parent merges it. Not yet measured: the benchmark commands are at the end.

### Short answer

The first version lost 35% to 40% at 8 callers because it let only one (or two) transactions of grouped dispatches run at a time, while the version without grouping ran one transaction per caller side by side. The new version never reduces how many transactions run at once. It groups only callers that would have waited for a producer slot anyway, and every free slot still carries a transaction. With fewer callers than slots it behaves exactly like no grouping, so it cannot lose there by design. It can only help when there are more callers than slots, which is when callers used to queue.

### Why the first version lost

- **One group in flight serialized the callers (the main cause).** The bench gives the producer one slot per caller (`MaxConcurrentCalls = calls`), so without grouping 8 callers had 8 transactions in flight, and PostgreSQL already flushes their commits to disk together. With grouping, every plain dispatch, including the lone one, counted against a limit of one transaction in flight (two in the "g2" arm). 8 callers became one transaction of about 4.5 commands at a time: fewer commits in flight, not fewer per job.
- **Waiters paid two round trips.** A caller that arrived during a send waited for that whole send to finish, then for its own group's send.
- **The collection wait was not the cause.** There was no timer; the queue filled while the previous send was on the wire.
- **The grouped SQL was not the main cause, but it still looped.** It wrote receipts with one `rw_receipt_v1` call per command. The new function writes them with one statement for the whole group.
- Two smaller costs: every group ran on a new background task, and the caller's admission lease was given back when the caller stopped waiting, even though its group was still in flight.

### What changed

- **Grouping moved in front of local admission (core, `DispatchGrouper`).** Local admission is the gate that already limits a process to `MaxConcurrentCalls` producer calls, with at most 64 waiting for up to 1 second. The PostgreSQL store never saw the waiting callers, so grouping there could only take slots away. Now:
  - A caller that finds a slot free sends at once, through the ordinary single path (`dispatch_v4`). Nobody waits to collect company.
  - A caller that finds every slot busy waits for a slot exactly as before: same waiting limit, same wait time, same `LocalCapacityExceededException` on refusal.
  - When a slot frees, the waiting caller that gets it takes up to 8 waiting commands (2 MiB, counting each payload plus 2 KiB) with it in one transaction. The callers it took stop waiting for a slot.
  - The slot stays taken until the group's transaction ends, even if the caller that took it stops waiting. So at most one transaction per slot is in flight, as without grouping, and the admission lease no longer leaks while a group is in flight.
- **A store offers grouping through an optional interface** (`IGroupedDispatchStore`). Only PostgreSQL offers it, and only with `PostgresOptions.GroupProducerAdmission = true` (default off).
- **Set-based SQL** (migration `0028_dispatch_group`, schema revision 28, function `dispatch_group_v1`). The gates are checked once. One insert writes every receipt placeholder in command-ID order. One query checks the other retention class. Only a command whose receipt already existed (a replay or a conflict) goes through `rw_receipt_v1` on its own. Then come one insert each into jobs, payloads, generations and events, and one receipt finalization. The stored rows equal a single dispatch's. A test compares them column by column, receipt included.
- **Failure handling.**
  - A group the server refuses or rolls back (gates, a command that passed its deadline after the writes, a job ID collision, a lock timeout) is resent command by command under the original IDs.
  - A connection failure before COMMIT leaves every command not applied.
  - A lost COMMIT reply makes the outcome unknown for each command under its own ID. `ResolveAsync`, or resending the same prepared dispatch, returns the original receipt.
- **Never grouped:** dispatches with a uniqueness key, concurrency or rate scopes; `DispatchManyAsync`; calls on your own transaction; the outbox, schedules and management dispatch.
- **Bench flags:** `--pg-group` turns grouping on. `--producer-calls n` sets the producer slots independently of the caller count, which is needed to have more callers than slots. The result JSON gains `producerCalls` and `pgGroup` (groups, grouped commands, fallbacks, mean group size).

### Why it is safe

Every command keeps its own operation ID, fingerprint, deadline, receipt and reply. The function decides each command as `dispatch_v4` would, using one decision time for the group. A refused command keeps no receipt, as when a single dispatch rolls back.

Cancellation:
- A caller cancelled while waiting is removed from the queue, and nothing is sent for it.
- A caller cancelled after a group took its command gets `DispatchCanceledAfterSendException` with its own ID. The transaction still settles its receipt.

Concurrency is never above what it was without grouping: at most one transaction per producer slot.

### Tests

Nine tests in `GroupedAdmissionTests`. The client has one producer slot, held by a dispatch stopped at its commit by a fault barrier. They cover:
- Waiting callers commit as one group, with single-dispatch rows and receipts, and each command replays alone to its own receipt.
- A lone caller is sent at once and never waits or groups.
- Mixed outcomes in one group each get their own reply: fresh, replay, command conflict, expired, unregistered queue, fresh. Refused commands keep no receipt.
- A job ID collision rolls the group back and each command is sent alone.
- A caller cancelled while waiting sends nothing.
- A caller cancelled after its group was sent can resolve and resend its operation.
- A lost group reply is outcome unknown for each member under its own ID, and each resends to its receipt.
- A waiting caller refused by local admission sends nothing.
- Keyed dispatches and calls on your own transaction take the ordinary path.

Results: the first run passed 8 of 9. The mixed-outcomes test had put the same command ID in one group twice, which the function rightly refuses; the test was fixed. The rerun of `GroupedAdmissionTests` together with `MigrationTests` (which gained the three 0028 phase cases) passed 93 of 93. The full Core, Postgres and Contract suites have not been run yet.

### Not done

- No throughput measurement yet. The paired runs are for a Sonnet agent; see below. The quick check of grouped versus single SQL cost per item was not run. `--trace` on the bench reports per-function server time (`dispatch_group_v1` against `dispatch_v4`).
- Storage telemetry records one `Dispatch` operation per group.
- The migration is numbered 0028 on the assumption that 0027 is the delayed-wake migration. Renumber at merge if needed.
- `ManagementSession` dispatch shares the same admission gate but is never grouped.
