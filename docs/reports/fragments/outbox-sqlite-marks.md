# Outbox on a SQLite source: batched delivered marks

**Short answer.** The SQLite outbox source now batches delivered marks the same way the PostgreSQL source does. Marks that wait behind each other are written in one statement and one commit, and each row still checks its own lease token. The option `BatchDeliveredMarks` (already on by default) now applies to SQLite too. Nothing changes in the schema.

## What changed

- `SqliteOutboxSource.MarkDeliveredAsync`: each mark joins a queue. Whoever holds the control lane next writes every queued mark (up to 64) in one `UPDATE … FROM (VALUES …)` inside one write transaction, then commits. A mark that is alone in the queue runs the old single-row statement.
- The relay registration (`AddRunwickSqliteOutbox`) passes `BatchDeliveredMarks` to the source. Setting it to false gives back one statement per mark.
- The single-row guarded update and the "open, begin, commit" steps are now small shared helpers, so renew, transport, retry and block run exactly the SQL they ran before.

## Why it is safe

- Every row is updated only if its `state` is still `leased` and its `lease_token` matches the caller's token. A relay that lost its lease gets `false` for its own message, its row stays as it is, and other marks in the same statement are not affected.
- A caller that gives up (is cancelled) before its mark is taken writes nothing; the next holder of the lane skips it.
- A caller cancelled while it holds the lane rolls the transaction back. Its own mark reports cancelled. The other marks go back in the queue and are written on the next turn. A repeat is harmless because of the token check.
- Any other failure reports the outcome as unknown for every mark in that transaction, the same as one failed mark today. The destination's mapping makes a retry safe.

## Tests

New class `tests/Runwick.Outbox.Tests/SqliteBatchedMarkTests.cs`:

- 12 concurrent marks with one stale token: 11 are written, possibly in shared statements; the stale one returns false and its row stays leased; afterwards the real owner can still mark it.
- A cancelled caller writes nothing, and a lone mark uses the single-row path (no batch statement is counted).
- A relay with four slots and batched marks delivers 24 messages exactly once (24 jobs, 24 mappings).
- A timing test that runs only when the environment variable `RUNWICK_OUTBOX_SQLITE_BENCH` is set to a message count. It stages that many messages and measures delivered per second while a four-slot relay drains them, with batching on and off.

All three contract tests passed. A smoke run of the timing test with 40 messages (not a measurement) showed 54 delivered/s with batching and 19 without.

## Not done

- Lease renewals are still one statement per lease. They happen periodically and are much less frequent than marks, so batching them is not worth the extra queue.
- The P-09 workload (`tests/Runwick.Workloads`, G2 roles) supports only a PostgreSQL source, so there is no paired P-09 run on SQLite. The drain timing test above is the measurement for SQLite.
- Full test projects and the timing run are still to be run.
