## PostgreSQL grouped producer admission (B1)

Written 2026-11-15. This is a fragment for `perf-round-2.md`; the parent merges it.

### What was built

`PostgresOptions.GroupProducerAdmission` (default off) lets one process commit several independent `DispatchAsync` calls in one PostgreSQL transaction.

- **How callers are grouped.** A caller that finds nothing in flight sends its own command at once and waits for no one. Callers that arrive while a send is in flight wait in a small queue (at most 8 commands and 2 MiB, counting each payload plus 2 KiB) and leave together when the slot frees. There is no timer, so the "at most 2 ms collection wait" in the plan is never spent: the queue fills while the previous send is on the wire. A command that does not fit the queue takes the ordinary path. The number of groups in flight is 1 by default; 2 is available for measurement through an internal setting.
- **What is grouped.** Only plain `Dispatch/1`: no uniqueness key, no concurrency or rate scope, no schedule header. Batches, calls on your own transaction, the outbox, schedules and management dispatch never join a group.
- **The database side.** Migration `0023_dispatch_group` (schema revision 23) adds `dispatch_group_v1`. It takes the gates once, then checks each command's receipt in command-ID order with the existing receipt helper. A replay, a conflict, an expired deadline, a bad request or an unregistered queue answers for that command alone, and the placeholder it inserted is removed. The rest share one decision time and insert their jobs, payloads, first generation and one event each, in the same shape a single dispatch writes. A parity test compares the stored rows of a grouped job with a single job's, apart from IDs and times.
- **Receipts and recovery.** Every command keeps its own operation ID, receipt, deadline and result. If anything fails before COMMIT is sent (including a member reaching its deadline after the writes, or a job ID collision), the whole group rolls back and every command retries alone with its original identity. If COMMIT was sent and the reply is lost, each command gets an outcome-unknown error carrying its own command ID, and `ResolveAsync` or a resend returns the original receipt.
- **Cancellation.** A caller that cancels while queued leaves the queue and nothing is sent for it. A caller that cancels after its group was sent stops waiting with its recoverable operation ID, and the transaction still settles its receipt.
- **Shutdown.** Disposing the store fails queued commands as not applied and drains groups already sent (cancelled after 10 seconds).

### Tests

Nine focused tests in `GroupedAdmissionTests` pass: one group with single-dispatch row parity and individual replays; a lone caller never forms a group; six mixed outcomes in one group (fresh, replay, conflict, expired, unregistered queue, fresh) with no receipt left for refused members; a job-ID collision rolling the group back and three commands retrying alone; cancellation while queued; cancellation after submission and resolving by ID; a lost reply giving outcome unknown to all members and replays on resend; disposal; and non-plain commands skipping the queue. The whole PostgreSQL project passes (403 tests; two timed out under the host's heavy load and passed alone), the Contract project passes 480 of 480, and the migration tests cover the new migration. The Core project passes 440 of 441; the one failure (`WorkerEngineTests.Hung_disposal_latches_unconfirmed_completion_without_a_retryable_finish`) also fails on the main checkout's build, so it does not come from this change.

### Measurement

Single-job `DispatchAsync` from 1, 8 and 16 callers, grouping off against on with one group in flight (g1) and with two (g2), alternating arm order for each pair, 3 pairs at 1 KiB and 2 pairs at 64 KiB and 1 MiB. Every run took `/tmp/runwick-bench.lock`.

**These numbers are informational and very noisy.** The host was running the SP-029 qualification and other agents' work, with a load average between 19 and 54 on 8 cores, and a single caller managed only 40 to 50 jobs/s. Rates are the median of the pairs; the ratio is the median of the per-pair grouped ÷ off ratios, with each pair in brackets.

| Payload | Callers | Off (jobs/s) | g1 (jobs/s), mean group | g1 ÷ off | g2 (jobs/s), mean group | g2 ÷ off |
|---|---|---|---|---|---|---|
| 1 KiB | 1 | 48.8 | 39.2, none formed | 0.92 [1.26, 0.62, 0.92] | n/a | n/a |
| 1 KiB | 8 | 187.1 | 113.3, 4.5 | 0.63 [0.63, 0.90, 0.58] | 108.8, 4.9 | 0.60 [0.60, 1.04, 0.56] |
| 1 KiB | 16 | 231.8 | 212.5, 8.0 | 0.66 [0.57, 1.04, 0.66] | 322.8, 5.6 | 1.57 [1.66, 1.57, 0.87] |
| 64 KiB | 1 | 40.8 | 44.9, none formed | 1.10 [1.09, 1.12] | n/a | n/a |
| 64 KiB | 8 | 167.4 | 99.3, 4.1 | 0.60 [0.72, 0.47] | 124.5, 3.4 | 0.74 [0.55, 0.93] |
| 64 KiB | 16 | 168.3 | 132.7, 8.0 | 0.79 [0.81, 0.77] | 185.2, 5.5 | 1.12 [1.29, 0.94] |
| 1 MiB | 8 | 36.5 | 31.7, none formed | 0.88 [0.60, 1.16] | 36.0, none formed | 0.98 [1.06, 0.91] |

A 1 MiB payload leaves room for one command in the 2 MiB queue, so no group ever formed there and those rows only show the cost of the extra bookkeeping. WAL per job is not reported: the host-wide WAL counter included other databases' writes.

### Reading

- Grouping with one group in flight lost throughput at 8 callers (a ratio of 0.58 to 0.90 at 1 KiB, 0.47 to 0.72 at 64 KiB) and at 16 callers with one group in flight (a median of 0.66 at 1 KiB). The off arm has up to 8 or 16 connections committing at once, and one group at a time serializes them.
- Two groups in flight did better at 16 callers: 1.57 times at 1 KiB (two pairs near 1.6, one at 0.87) and 1.12 times at 64 KiB (1.29 and 0.94). That is the only cell where grouping looks useful, and three pairs on a loaded host cannot confirm it. At 8 callers two in flight still lost (0.60 and 0.74).
- A lone caller was neither helped nor held back beyond noise: no group formed, and the ratio at 1 KiB moved between 0.62 and 1.26 pair to pair.
- No fallbacks happened in any run, so the rollback path costs nothing in the measured mix.

### What stays open

- The measurement needs a quiet host with a separate database host before grouped admission is recommended. Until then it stays off by default and unrecommended; the plan's gate (lower SQL or commit cost under concurrency with no singleton latency regression) is not met at 8 callers.
- Whether two groups in flight is the right default at 16 or more callers, and whether the producer lane size should change with it.
- Server-side fault points (`BeforeFirstMutation`, `AfterMutation`) are not wired into the group function. Tests use the client fault points before, at and after COMMIT, applied to the group's first command.
- The admission lease still belongs to the caller. A caller that stops waiting releases its lease while its group is in flight. The abandoned work is bounded (at most 8 commands, one or two groups) rather than tracked per caller.
- Storage telemetry records one `Dispatch` operation per group, not one per command.
- Raw runs: `artifacts/perf/round-two/pg-b1-group/` in the worktree (`pairs.sh`, `summarize.py`, `summary.json`, the 46 result files). Git ignores it.
