using Microsoft.Data.Sqlite;
using Runwick.Dispatching;
using Runwick.Hosting;
using Runwick.Protocol;

namespace Runwick.Sqlite.Store;

/// <summary>
/// Caller-owned staging (20 §8, as PostgresJobDispatcher): Dispatch/1 and DispatchMany/1 on the caller's transaction and
/// thread. No local admission, no unknown-outcome exceptions (the commit is the caller's), no retries.
/// </summary>
internal sealed class SqliteJobDispatcher(RunwickRuntime runtime, DispatchPreparer preparer, SqliteStore store) : ISqliteJobDispatcher
{
    public async Task<StagedDispatch> StageAsync<TJob>(TJob job, SqliteTransaction transaction, DispatchOptions? options = null,
        CancellationToken cancellationToken = default) where TJob : class, IQueueJob
    {
        Validate(transaction);
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        return Send(preparer.Prepare(runtime.StoreId, job, options), transaction, cancellationToken);
    }

    public async Task<StagedDispatch> StageAsync(PreparedDispatch prepared, SqliteTransaction transaction, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(prepared);
        Validate(transaction);
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        return Send(prepared, transaction, cancellationToken);
    }

    public async Task<StagedBatch> StageManyAsync(IReadOnlyList<BatchDispatchItem> items, SqliteTransaction transaction,
        BatchDispatchOptions? options = null, CancellationToken cancellationToken = default)
    {
        Validate(transaction);
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        return Send(preparer.PrepareMany(runtime.StoreId, items, options), transaction, cancellationToken);
    }

    public async Task<StagedBatch> StageManyAsync(PreparedBatch prepared, SqliteTransaction transaction, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(prepared);
        Validate(transaction);
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        return Send(prepared, transaction, cancellationToken);
    }

    static void Validate(SqliteTransaction transaction)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        if (transaction.Connection is null)
        {
            throw new TransactionValidationException(RunwickErrorCodes.TransactionInvalid, "The transaction is completed or has no connection.");
        }
    }

    StagedDispatch Send(PreparedDispatch prepared, SqliteTransaction transaction, CancellationToken ct)
    {
        var command = new StoreCommand<DispatchRequest>(prepared.Application, prepared.OperationId.Value, prepared.NotAfter,
            new DispatchRequest(new DispatchItem(prepared.Intent, prepared.Scopes, prepared.Digests) { Rates = preparer.RatesFor(prepared.Intent) }, prepared.StoreId, prepared.Digests.Command));
        using var span = StageSpan(prepared.Intent.Queue, 1);
        var reply = Guard(() => store.StageDispatch(transaction, command, ct), span.Activity);
        if (reply.Status == StoreStatus.Applied && reply.Value is { } r)
        {
            span.Activity?.SetTag("runwick.job.id", r.JobId.ToString("D")).SetTag("messaging.message.id", r.JobId.ToString("D"));
            return Staged(prepared.OperationId, r);
        }

        throw Failed(span.Activity, Rejection(reply, prepared.OperationId, prepared.NotAfter, prepared, null));
    }

    StagedBatch Send(PreparedBatch prepared, SqliteTransaction transaction, CancellationToken ct)
    {
        var items = prepared.Items.Select((intent, i) => new DispatchItem(intent, prepared.Scopes[i], prepared.ItemDigests[i])
        {
            Rates = preparer.RatesFor(intent),
        }).ToList();
        var command = new StoreCommand<DispatchBatchRequest>(prepared.Application, prepared.OperationId.Value, prepared.NotAfter,
            new DispatchBatchRequest(items, prepared.ContinueOnConflict, prepared.StoreId, prepared.CommandDigest));
        using var span = StageSpan(prepared.Items.Count > 0 ? prepared.Items[0].Queue : "", prepared.Items.Count);
        var reply = Guard(() => store.StageDispatchMany(transaction, command, ct), span.Activity);
        if (reply.Status == StoreStatus.Applied && reply.Value is { } r)
        {
            return new StagedBatch(prepared.OperationId, [.. r.Items.Select(i => new StagedBatchItemResult(
                i.Ordinal,
                i.Result is { } ok ? Staged(prepared.OperationId, ok) : null,
                i.Failure is { } f ? new BatchItemFailure(i.Ordinal, f.Code, f.Detail ?? f.Code, f.ConflictingJobId is { } c ? new JobId(c) : null) : null))]);
        }

        throw Failed(span.Activity, Rejection(reply, prepared.OperationId, prepared.NotAfter, null, prepared));
    }

    /// <summary>The <c>stage {queue}</c> span (18 §2.1), as PostgresJobDispatcher: never <c>created</c>, the job exists only if the caller commits.</summary>
    static Telemetry.Span StageSpan(string queue, int count)
    {
        var activity = Telemetry.Telemetry.StartActivity("stage " + queue, System.Diagnostics.ActivityKind.Producer);
        activity?.SetTag("runwick.dispatch.kind", "staged").SetTag("messaging.system", "runwick").SetTag("messaging.operation.type", "send")
            .SetTag("messaging.operation.name", "stage").SetTag("messaging.destination.name", queue);
        if (count > 1)
        {
            activity?.SetTag("messaging.batch.message_count", count);
        }

        return new Telemetry.Span(activity);
    }

    static Exception Failed(System.Diagnostics.Activity? span, Exception ex)
    {
        span?.SetTag("error.type", ex is RunwickException r ? r.Code : ex.GetType().FullName).SetStatus(System.Diagnostics.ActivityStatusCode.Error);
        return ex;
    }

    static StoreReply<T> Guard<T>(Func<StoreReply<T>> call, System.Diagnostics.Activity? span) where T : class
    {
        try
        {
            return call();
        }
        catch (SqStagingValidationException ex)
        {
            throw Failed(span, new TransactionValidationException(ex.Code, ex.Message));
        }
    }

    static StagedDispatch Staged(DispatchOperationId operationId, DispatchResult r) =>
        new(new JobId(r.JobId), operationId, r.Disposition, r.Match, r.PolicyDrift);

    static Exception Rejection<T>(StoreReply<T> reply, DispatchOperationId operationId, DateTimeOffset notAfter,
        PreparedDispatch? prepared, PreparedBatch? batch) where T : class =>
        reply.Error?.Code == "store_identity_mismatch"
            ? new TransactionValidationException(RunwickErrorCodes.TransactionStoreMismatch,
                "The transaction's database is not the configured Runwick store.")
            : JobDispatcher.Rejection(reply.Status, reply.Error, reply.DecidedAt, operationId, notAfter, prepared, batch);
}
