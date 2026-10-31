using Npgsql;

namespace Runwick.Postgres;

/// <summary>
/// Transactional completion (R6, 01 §16, 08 §8): Finish/1 <c>Succeeded</c> staged inside a caller-owned transaction so
/// same-database business effects and the job's completion commit together or not at all. Resolvable only inside an
/// attempt's scope; at most one staging per attempt. Runwick never commits, rolls back or retries the transaction.
/// </summary>
public interface IPostgresJobCompletion
{
    /// <exception cref="JobCompletionException"><c>no_active_attempt</c> or <c>completion_already_staged</c>.</exception>
    /// <exception cref="JobOwnershipLostException">The attempt no longer owns the job; roll the transaction back.</exception>
    /// <exception cref="TransactionValidationException">The transaction cannot host staging or belongs to another store.</exception>
    Task<StagedCompletion> CompleteInTransactionAsync(NpgsqlTransaction transaction, CancellationToken cancellationToken = default);
}
