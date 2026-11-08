using Microsoft.Data.Sqlite;
using Runwick.Protocol;
using Runwick.Worker;

namespace Runwick.Sqlite.Store;

/// <summary>The scoped R6 service (20 §8, as PostgresJobCompletion). Outside a worker attempt scope every call is <c>no_active_attempt</c>.</summary>
internal sealed class SqliteJobCompletion(IServiceProvider scope, SqliteStore store) : ISqliteJobCompletion
{
    public Task<StagedCompletion> CompleteInTransactionAsync(SqliteTransaction transaction, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        var holder = scope.GetService(typeof(JobContextHolder)) as JobContextHolder;
        if (holder is not { Entry: { } entry, Engine: { } engine })
        {
            throw new JobCompletionException(RunwickErrorCodes.NoActiveAttempt, "Transactional completion is available only inside a job attempt.");
        }

        if (transaction.Connection is null)
        {
            throw new TransactionValidationException(RunwickErrorCodes.TransactionInvalid, "The transaction is completed or has no connection.");
        }

        var command = engine.PrepareStagedCompletion(entry);
        StoreReply<FinishedAttempt> reply;
        try
        {
            reply = store.StageFinish(transaction, command, cancellationToken);
        }
        catch (SqStagingValidationException ex)
        {
            engine.AbandonStagedCompletion(entry, ownershipLost: false);
            throw new TransactionValidationException(ex.Code, ex.Message);
        }
        catch
        {
            // SQLite errors (SQLITE_BUSY/BUSY_SNAPSHOT/...) propagate unchanged; the caller's transaction is the caller's to end.
            engine.AbandonStagedCompletion(entry, ownershipLost: false);
            throw;
        }

        switch (reply.Status)
        {
            case StoreStatus.Applied:
                engine.RecordStagedCompletion(entry, command, () => transaction.Connection is not null);
                return Task.FromResult(new StagedCompletion(new JobId(entry.Lease.JobId), new AttemptId(entry.AttemptId), command.CommandId));
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
}
