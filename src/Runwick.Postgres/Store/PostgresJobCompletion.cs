using Npgsql;
using Runwick.Protocol;
using Runwick.Worker;

namespace Runwick.Postgres.Store;

/// <summary>The scoped R6 service (08 §8.1). Outside a worker attempt scope every call is <c>no_active_attempt</c>.</summary>
internal sealed class PostgresJobCompletion(IServiceProvider scope, PgStore store) : IPostgresJobCompletion
{
    public async Task<StagedCompletion> CompleteInTransactionAsync(NpgsqlTransaction transaction, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        var holder = scope.GetService(typeof(JobContextHolder)) as JobContextHolder;
        if (holder is not { Entry: { } entry, Engine: { } engine })
        {
            throw new JobCompletionException(RunwickErrorCodes.NoActiveAttempt, "Transactional completion is available only inside a job attempt.");
        }

        if (!IsOpen(transaction))
        {
            throw new TransactionValidationException(RunwickErrorCodes.TransactionInvalid, "The transaction is completed or has no connection.");
        }

        var command = engine.PrepareStagedCompletion(entry);
        StoreReply<FinishedAttempt> reply;
        try
        {
            reply = await store.StageFinishAsync(transaction, command, cancellationToken).ConfigureAwait(false);
        }
        catch (StagingValidationException ex)
        {
            engine.AbandonStagedCompletion(entry, ownershipLost: false);
            throw new TransactionValidationException(ex.Code, ex.Message);
        }
        catch
        {
            // PostgreSQL errors (40001/40P01/...) propagate unchanged; the caller's transaction is the caller's to end.
            engine.AbandonStagedCompletion(entry, ownershipLost: false);
            throw;
        }

        switch (reply.Status)
        {
            case StoreStatus.Applied:
                engine.RecordStagedCompletion(entry, command, () => IsOpen(transaction));
                return new StagedCompletion(new JobId(entry.Lease.JobId), new AttemptId(entry.AttemptId), command.CommandId);
            case StoreStatus.StaleLease or StoreStatus.InvalidState or StoreStatus.NotFound:
                engine.AbandonStagedCompletion(entry, ownershipLost: true);
                throw new JobOwnershipLostException($"The attempt no longer owns job {entry.Lease.JobId} ({reply.Error?.Code}); roll the transaction back.");
            default:
                engine.AbandonStagedCompletion(entry, ownershipLost: false);
                throw new TransactionValidationException(
                    reply.Error?.Code == "store_identity_mismatch" ? RunwickErrorCodes.TransactionStoreMismatch : reply.Error?.Code ?? RunwickErrorCodes.TransactionInvalid,
                    $"Transactional completion was rejected: {reply.Status} {reply.Error?.Code}.");
        }
    }

    static bool IsOpen(NpgsqlTransaction transaction)
    {
        try
        {
            return transaction.Connection is not null;
        }
        catch (InvalidOperationException)
        {
            return false; // Npgsql throws for a completed transaction.
        }
    }
}
