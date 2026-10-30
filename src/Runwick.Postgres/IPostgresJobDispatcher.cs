using Npgsql;

namespace Runwick.Postgres;

/// <summary>Stages dispatches inside a caller-owned PostgreSQL transaction (spec 01 §15).</summary>
public interface IPostgresJobDispatcher
{
    Task<StagedDispatch> StageAsync<TJob>(
        TJob job,
        NpgsqlTransaction transaction,
        DispatchOptions? options = null,
        CancellationToken cancellationToken = default)
        where TJob : class, IQueueJob;

    Task<StagedDispatch> StageAsync(
        PreparedDispatch prepared,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken = default);

    Task<StagedBatch> StageManyAsync(
        IReadOnlyList<BatchDispatchItem> items,
        NpgsqlTransaction transaction,
        BatchDispatchOptions? options = null,
        CancellationToken cancellationToken = default);

    Task<StagedBatch> StageManyAsync(
        PreparedBatch prepared,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken = default);
}
