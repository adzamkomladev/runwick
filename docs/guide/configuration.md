# Configuration reference

Defaults below are checked against the code by a test. Times are as written; the full tables, bounds and the rules that tie the timings together are in [docs/spec/06-defaults.md](../spec/06-defaults.md). Runwick checks the timing rules at startup and refuses a configuration that breaks them; it never quietly adjusts a value.

## Per job (attribute or fluent, later layers win)

| Setting | Default |
|---|---|
| `MaxExecutions` (counts real starts, so 3 means two automatic retries) | 3 |
| `ExecutionTimeout` | none |
| Retry delay (exponential, half-random) | 5 s first, never more than 60 s |
| Re-queue after a worker took a job but never started it | up to 5 times, 1 s growing to 30 s |
| Queue / priority | `default` / medium |
| Uniqueness, rate limits, concurrency caps | off until you declare them |

## Worker (`WorkerOptions`)

| Setting | Default |
|---|---|
| Lease length / safety margin | 60 s / 10 s |
| Lease renewal | every 15 s, plus or minus 3 s |
| Control request timeout | 5 s |
| Minimum time left on a lease to start a handler | 15 s |
| Cancellation grace | 10 s |
| Cleanup budget | 10 s |
| Crashed-worker recovery scan | every 5 s |
| Shutdown (total / drain / cancel / finish) | 25 s / 15 s / 5 s / 5 s |
| Pool size | fixed 4 (`FixedConcurrency(n)`) or automatic (`AutoConcurrency(min, max)`) |
| Most jobs one process holds at once | 16 |
| Lane weights high : medium : low | 6 : 3 : 1, with aging after 60 s |

Keep the host's own shutdown timeout (30 s by default in .NET) above Runwick's 25 s.

## Producer (`ProducerOptions`)

Concurrent dispatch calls 8, waiting callers 64, longest wait for a slot 1 s, prepared dispatch valid for 5 min.

## Stores

| Option | Default | Allowed |
|---|---|---|
| PostgreSQL schema | `runwick` | only `runwick` |
| SQLite `WriterBusyTimeout` | 2 s | 100 ms to 30 s |
| SQLite `ReaderThreads` | 2 | 1 to 8 |
| Redis `ProfileRecheckInterval` | 30 s | 5 s to 5 min |

## Retention (`w.Retention(...)` on the worker)

Enabled; batches of 100 rows, 1 MiB of detail, aiming at 500 ms; 1 s pause between batches; idle check every 60 s. Cleanup is bounded work, not a promise to keep up with any load. Deleting rows does not erase backups or copies.

## Scheduler (`AddScheduler`)

Checks every 1 s while work is due and backs off to 5 s when idle; at most 100 jobs per second per scheduler process, burst 32.

## Outbox relay (`OutboxRelayOptions`)

4 messages in flight, 60 s lease renewed every 15 s, 5 s provider call timeout, idle poll 1 s, retry 1 s growing to 60 s.

## Management and dashboard

Management needs authentication schemes and a grant mapping or it will not start; there is no anonymous mode. See [management](management.md). Dashboard title defaults to `Runwick`.

## Payload and name limits (fixed)

Payload 1 MiB of JSON, 32 levels deep; names 1 to 128 characters (`[A-Za-z0-9][A-Za-z0-9._-]*`, job names up to 200); uniqueness and resource keys up to 512 bytes; a batch dispatch up to 100 items or 2 MiB.
