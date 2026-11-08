# What Runwick promises (and does not)

## At-least-once

A job's handler can run more than once. This happens if a worker crashes after doing the work but before recording it, if a worker loses its lease while still running, or if a reply is lost. Runwick retries rather than risk losing a job.

So write handlers that are safe to repeat.

## Make repeats harmless: idempotency keys

When your handler talks to something outside Runwick's database (an email service, a payment API, another database), pass a key that is the same on every repeat and let the other side ignore duplicates. A good key is stable and comes from the job: for example the job ID (`IJobContext.JobId`) or a business ID in the payload. Many APIs accept an `Idempotency-Key` header for exactly this.

Two related but different things:

- **Dispatch uniqueness** (`UniqueWhileActive`, `UniqueWithinWindow`) stops two *jobs* being created for the same key. It prevents duplicate insertion. It does not stop one job's handler running twice.
- **Idempotency keys** make the *effects* of a repeated run harmless.

## The one exactly-once case: same database

If your handler's effect is a change in the same database as Runwick's tables (PostgreSQL or SQLite), you can make the change and the job's completion commit together:

```csharp
await using var tx = await connection.BeginTransactionAsync(ct);
// ... your business changes on tx ...
await completion.CompleteInTransactionAsync(tx, ct);    // IPostgresJobCompletion or ISqliteJobCompletion
await tx.CommitAsync(ct);
```

Either both are saved or neither. If the worker no longer owns the job, `CompleteInTransactionAsync` throws; roll back. Runwick never commits or rolls back your transaction for you. The same pattern with `StageAsync` lets you create a job in the same transaction as your own data.

This guarantee covers effects inside that one database only. It does not cover emails, HTTP calls, other databases, or Redis. Redis has no transactional completion at all.

## The outbox

To create a job in a different Runwick store from your business database, use the outbox (`Runwick.Outbox`): your data and an outbox message commit together, and a relay delivers the message to the store. Delivery is at-least-once, with de-duplication on the receiving side for a limited time window. It is not exactly-once dispatch, it does not preserve order, and de-duplication does not hold after the window or after restoring the destination from a backup. The SQLite outbox is a source for same-machine use only.

## Other things Runwick does not claim

- A lease running out does not prove the old handler stopped. Two copies can briefly overlap. Concurrency limits count valid leases, not code that is physically still running.
- "Began" does not prove your code ran; a crash right after is labelled abandoned.
- Timeouts measure the handler pipeline, not what happened in the outside world. There is no default timeout.
- A restored database is a break in history: receipts, uniqueness windows and fences from before the restore are not guaranteed to behave as one continuous record.
- A schedule creates at most one job per occurrence, and that job is at-least-once like any other.
- Counts and metrics can be lost on a crash and are not for billing or audit.
- No claim is made for Native AOT, trimming, or operating systems other than those listed as tested (Linux x64 and Windows x64, .NET 10).
