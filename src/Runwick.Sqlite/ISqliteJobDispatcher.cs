using Microsoft.Data.Sqlite;

namespace Runwick.Sqlite;

/// <summary>
/// Stages dispatches inside a caller-owned SQLite transaction on the store's own file (20 §8, 01 §15): the job, its keys
/// and its receipt commit or roll back with the caller's business rows. Runwick never commits, rolls back, disposes or
/// retries the transaction; a <c>SQLITE_BUSY</c> from a deferred transaction propagates unchanged, so begin business
/// transactions that stage with <c>BeginTransaction(deferred: false)</c> and keep them short (every Runwick write on the
/// file waits while one is open).
/// </summary>
/// <remarks>
/// The caller's connection must open the configured database file with <c>PRAGMA foreign_keys = 1</c> and
/// <c>PRAGMA synchronous = FULL</c>; otherwise staging throws <see cref="TransactionValidationException"/> before any write
/// (<c>transaction_store_mismatch</c>, <c>transaction_profile_mismatch</c> or <c>transaction_invalid</c>).
/// </remarks>
public interface ISqliteJobDispatcher
{
    /// <summary>Prepares <paramref name="job"/> and stages one Dispatch/1 on <paramref name="transaction"/>.</summary>
    /// <exception cref="TransactionValidationException">The transaction cannot host staging or belongs to another store.</exception>
    Task<StagedDispatch> StageAsync<TJob>(
        TJob job,
        SqliteTransaction transaction,
        DispatchOptions? options = null,
        CancellationToken cancellationToken = default)
        where TJob : class, IQueueJob;

    /// <summary>Stages an already prepared dispatch; a retry of the whole caller transaction reuses the same value.</summary>
    /// <exception cref="TransactionValidationException">The transaction cannot host staging or belongs to another store.</exception>
    Task<StagedDispatch> StageAsync(
        PreparedDispatch prepared,
        SqliteTransaction transaction,
        CancellationToken cancellationToken = default);

    /// <summary>Prepares <paramref name="items"/> and stages one DispatchMany/1 on <paramref name="transaction"/>.</summary>
    /// <exception cref="TransactionValidationException">The transaction cannot host staging or belongs to another store.</exception>
    Task<StagedBatch> StageManyAsync(
        IReadOnlyList<BatchDispatchItem> items,
        SqliteTransaction transaction,
        BatchDispatchOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>Stages an already prepared batch.</summary>
    /// <exception cref="TransactionValidationException">The transaction cannot host staging or belongs to another store.</exception>
    Task<StagedBatch> StageManyAsync(
        PreparedBatch prepared,
        SqliteTransaction transaction,
        CancellationToken cancellationToken = default);
}
