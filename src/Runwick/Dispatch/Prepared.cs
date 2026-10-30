using Runwick.Serialization;

namespace Runwick;

/// <summary>An immutable, validated single dispatch: retrying it replays the same command.</summary>
public sealed class PreparedDispatch
{
    internal PreparedDispatch(Guid storeId, string application, Guid operationId, long notAfterMs, DispatchIntent intent, DispatchDigests digests)
    {
        StoreId = storeId;
        Application = application;
        OperationId = new DispatchOperationId(operationId);
        NotAfterMs = notAfterMs;
        Intent = intent;
        Digests = digests;
    }

    public DispatchOperationId OperationId { get; }

    public DateTimeOffset NotAfter => DateTimeOffset.FromUnixTimeMilliseconds(NotAfterMs);

    public string Application { get; }

    public string JobName => Intent.JobName;

    public int PayloadVersion => Intent.PayloadVersion;

    public string Queue => Intent.Queue;

    public string Priority => Intent.Priority;

    internal Guid StoreId { get; }

    internal long NotAfterMs { get; }

    internal DispatchIntent Intent { get; }

    internal DispatchDigests Digests { get; }

    /// <summary>Scope membership with seed capacities (not part of the envelope).</summary>
    internal IReadOnlyList<Protocol.ScopeRequirement> Scopes { get; init; } = [];

    /// <summary>The versioned export envelope (spec 02 §9.1). Contains business data; keep it out of logs.</summary>
    public ReadOnlyMemory<byte> Export() => PreparedEnvelopeCodec.Write(this);

    public override string ToString() => $"PreparedDispatch {OperationId} {JobName}";
}

/// <summary>An immutable, validated batch dispatch: one command, one transaction, one receipt.</summary>
public sealed class PreparedBatch
{
    internal PreparedBatch(
        Guid storeId, string application, Guid operationId, long notAfterMs, bool continueOnConflict,
        IReadOnlyList<DispatchIntent> items, IReadOnlyList<DispatchDigests> itemDigests, byte[] commandDigest)
    {
        StoreId = storeId;
        Application = application;
        OperationId = new DispatchOperationId(operationId);
        NotAfterMs = notAfterMs;
        ContinueOnConflict = continueOnConflict;
        Items = items;
        ItemDigests = itemDigests;
        CommandDigest = commandDigest;
    }

    public DispatchOperationId OperationId { get; }

    public DateTimeOffset NotAfter => DateTimeOffset.FromUnixTimeMilliseconds(NotAfterMs);

    public string Application { get; }

    public int Count => Items.Count;

    internal Guid StoreId { get; }

    internal long NotAfterMs { get; }

    internal bool ContinueOnConflict { get; }

    internal IReadOnlyList<DispatchIntent> Items { get; }

    internal IReadOnlyList<DispatchDigests> ItemDigests { get; }

    internal byte[] CommandDigest { get; }

    internal IReadOnlyList<IReadOnlyList<Protocol.ScopeRequirement>> Scopes { get; init; } = [];

    public ReadOnlyMemory<byte> Export() => PreparedEnvelopeCodec.Write(this);

    public override string ToString() => $"PreparedBatch {OperationId} ({Count} items)";
}

public enum DispatchResolutionStatus
{
    Confirmed,
    NotObserved,
    Recovering,
    ExpiredUnknown,
}

public sealed record DispatchResolution(
    DispatchOperationId OperationId,
    DispatchResolutionStatus Status,
    DispatchReceipt? Receipt,
    BatchDispatchReceipt? BatchReceipt);

/// <summary>Prepare-then-send dispatch with explicit operation identity and recovery.</summary>
public interface IPreparedJobDispatcher
{
    PreparedDispatch Prepare<TJob>(TJob job, DispatchOptions? options = null)
        where TJob : class, IQueueJob;

    PreparedBatch PrepareMany(
        IReadOnlyList<BatchDispatchItem> items,
        BatchDispatchOptions? options = null);

    Task<DispatchReceipt> DispatchAsync(
        PreparedDispatch prepared,
        CancellationToken cancellationToken = default);

    Task<BatchDispatchReceipt> DispatchAsync(
        PreparedBatch prepared,
        CancellationToken cancellationToken = default);

    Task<DispatchResolution> ResolveAsync(
        DispatchOperationId operationId,
        CancellationToken cancellationToken = default);

    PreparedDispatch ImportPrepared(ReadOnlySpan<byte> envelope);

    PreparedBatch ImportPreparedBatch(ReadOnlySpan<byte> envelope);
}
