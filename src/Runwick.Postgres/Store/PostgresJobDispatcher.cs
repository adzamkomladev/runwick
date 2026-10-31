using Npgsql;
using Runwick.Dispatching;
using Runwick.Hosting;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

/// <summary>
/// Caller-owned staging (01 §15): Dispatch/1 and DispatchMany/1 on the caller's transaction. No local admission,
/// no unknown-outcome exceptions (the commit is the caller's), no retries, no other connection for the writes.
/// </summary>
internal sealed class PostgresJobDispatcher(RunwickRuntime runtime, DispatchPreparer preparer, PgStore store) : IPostgresJobDispatcher
{
    public async Task<StagedDispatch> StageAsync<TJob>(TJob job, NpgsqlTransaction transaction, DispatchOptions? options = null,
        CancellationToken cancellationToken = default) where TJob : class, IQueueJob
    {
        Validate(transaction);
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        return await SendAsync(preparer.Prepare(runtime.StoreId, job, options), transaction, cancellationToken).ConfigureAwait(false);
    }

    public async Task<StagedDispatch> StageAsync(PreparedDispatch prepared, NpgsqlTransaction transaction, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(prepared);
        Validate(transaction);
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        return await SendAsync(prepared, transaction, cancellationToken).ConfigureAwait(false);
    }

    public async Task<StagedBatch> StageManyAsync(IReadOnlyList<BatchDispatchItem> items, NpgsqlTransaction transaction,
        BatchDispatchOptions? options = null, CancellationToken cancellationToken = default)
    {
        Validate(transaction);
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        return await SendAsync(preparer.PrepareMany(runtime.StoreId, items, options), transaction, cancellationToken).ConfigureAwait(false);
    }

    public async Task<StagedBatch> StageManyAsync(PreparedBatch prepared, NpgsqlTransaction transaction, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(prepared);
        Validate(transaction);
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        return await SendAsync(prepared, transaction, cancellationToken).ConfigureAwait(false);
    }

    static void Validate(NpgsqlTransaction transaction)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        NpgsqlConnection? connection;
        try
        {
            connection = transaction.Connection;
        }
        catch (InvalidOperationException)
        {
            connection = null; // Npgsql throws for a completed transaction.
        }

        if (connection is null)
        {
            throw new TransactionValidationException(RunwickErrorCodes.TransactionInvalid, "The transaction is completed or has no connection.");
        }
    }

    async Task<StagedDispatch> SendAsync(PreparedDispatch prepared, NpgsqlTransaction transaction, CancellationToken ct)
    {
        var command = new StoreCommand<DispatchRequest>(prepared.Application, prepared.OperationId.Value, prepared.NotAfter,
            new DispatchRequest(new DispatchItem(prepared.Intent, prepared.Scopes, prepared.Digests) { Rates = preparer.RatesFor(prepared.Intent) }, prepared.StoreId, prepared.Digests.Command));
        var reply = await Guard(() => store.StageDispatchAsync(transaction, command, ct)).ConfigureAwait(false);
        if (reply.Status == StoreStatus.Applied && reply.Value is { } r)
        {
            return Staged(prepared.OperationId, r);
        }

        throw Rejection(reply, prepared.OperationId, prepared.NotAfter, prepared, null);
    }

    async Task<StagedBatch> SendAsync(PreparedBatch prepared, NpgsqlTransaction transaction, CancellationToken ct)
    {
        var items = prepared.Items.Select((intent, i) => new DispatchItem(intent, prepared.Scopes[i], prepared.ItemDigests[i])
        {
            Rates = preparer.RatesFor(intent),
        }).ToList();
        var command = new StoreCommand<DispatchBatchRequest>(prepared.Application, prepared.OperationId.Value, prepared.NotAfter,
            new DispatchBatchRequest(items, prepared.ContinueOnConflict, prepared.StoreId, prepared.CommandDigest));
        var reply = await Guard(() => store.StageDispatchManyAsync(transaction, command, ct)).ConfigureAwait(false);
        if (reply.Status == StoreStatus.Applied && reply.Value is { } r)
        {
            return new StagedBatch(prepared.OperationId, [.. r.Items.Select(i => new StagedBatchItemResult(
                i.Ordinal,
                i.Result is { } ok ? Staged(prepared.OperationId, ok) : null,
                i.Failure is { } f ? new BatchItemFailure(i.Ordinal, f.Code, f.Detail ?? f.Code, f.ConflictingJobId is { } c ? new JobId(c) : null) : null))]);
        }

        throw Rejection(reply, prepared.OperationId, prepared.NotAfter, null, prepared);
    }

    static async Task<StoreReply<T>> Guard<T>(Func<Task<StoreReply<T>>> call) where T : class
    {
        try
        {
            return await call().ConfigureAwait(false);
        }
        catch (StagingValidationException ex)
        {
            throw new TransactionValidationException(ex.Code, ex.Message);
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
