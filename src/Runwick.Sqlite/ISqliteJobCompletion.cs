using Microsoft.Data.Sqlite;

namespace Runwick.Sqlite;

/// <summary>
/// Transactional completion (R6, 20 §8, 01 §16, 08 §8): Finish/1 <c>Succeeded</c> staged inside a caller-owned SQLite
/// transaction so same-file business effects and the job's completion commit together or not at all. Resolvable only
/// inside an attempt's scope; at most one staging per attempt. Runwick never commits, rolls back or retries the transaction,
/// and a <c>SQLITE_BUSY</c> propagates unchanged.
/// </summary>
public interface ISqliteJobCompletion
{
    /// <summary>Stages the current attempt's successful completion on <paramref name="transaction"/>.</summary>
    /// <exception cref="JobCompletionException"><c>no_active_attempt</c> or <c>completion_already_staged</c>.</exception>
    /// <exception cref="JobOwnershipLostException">The attempt no longer owns the job; roll the transaction back.</exception>
    /// <exception cref="TransactionValidationException">The transaction cannot host staging, belongs to another store or its
    /// connection does not have the store's profile (<c>transaction_profile_mismatch</c>).</exception>
    Task<StagedCompletion> CompleteInTransactionAsync(SqliteTransaction transaction, CancellationToken cancellationToken = default);
}
