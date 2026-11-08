# Cancelling jobs

Cancellation in Runwick is a request, not a kill. .NET cannot safely stop code that is already running, and Runwick does not try.

## What happens

- **Job not started yet:** cancelling moves it to `cancelled` and it never runs.
- **Job running:** the store records "cancel requested". The worker cancels the `CancellationToken` passed to your handler. Your code must watch the token and stop.
- If the handler ignores the token, it keeps running. After a grace period (default 10 seconds) Runwick stops waiting for it and records the outcome, but the code may still be running in the process.

The same token is cancelled for a timeout (`WithExecutionTimeout`), a lost lease and a worker shutdown. `IJobContext.CancellationCauses` says which of these Runwick has seen.

## Write handlers that cooperate

```csharp
public async Task HandleAsync(Report job, CancellationToken ct)
{
    foreach (var chunk in chunks)
    {
        ct.ThrowIfCancellationRequested();       // or pass ct to every awaited call
        await Process(chunk, ct);
    }
}
```

Pass the token to database calls, HTTP calls and delays. Make work you cannot interrupt short, and make each step safe to repeat (see [delivery semantics](delivery-semantics.md)).

## What the dashboard, CLI and API show

They show "cancel requested" until the worker reports the outcome. They never say a cancel "stopped the handler". A cancel on a job that has already finished is answered as such, not as success.
