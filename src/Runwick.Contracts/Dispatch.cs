namespace Runwick;

public enum DispatchDisposition
{
    Created,
    Reused,
}

public enum DuplicateMatch
{
    None,
    Active,
    Window,
    ActiveAndWindow,
}

/// <summary>Confirmation that a dispatch is durable under <see cref="ConfirmedAcknowledgementProfile"/>.</summary>
public sealed record DispatchReceipt(
    JobId JobId,
    DispatchOperationId OperationId,
    DispatchDisposition Disposition,
    DuplicateMatch DuplicateMatch,
    bool PolicyDrift,
    string ConfirmedAcknowledgementProfile);

public sealed record DispatchOptions
{
    /// <summary>A registered queue; recomputes the queue scope, keeps job-type and resource scopes.</summary>
    public string? Queue { get; init; }

    /// <summary><c>low</c>, <c>medium</c> or <c>high</c>.</summary>
    public string? Priority { get; init; }

    /// <summary>Eligibility bound, rounded up to the millisecond; not an exact start time.</summary>
    public DateTimeOffset? NotBefore { get; init; }

    public DispatchUniqueness? Uniqueness { get; init; }

    /// <summary>Business metadata: part of the window business digest; exposed as <see cref="IJobContext.Metadata"/>.</summary>
    public IReadOnlyDictionary<string, string>? Metadata { get; init; }

    public override string ToString() => $"DispatchOptions(Queue = {Queue}, Priority = {Priority}, NotBefore = {NotBefore:O})";
}

public sealed record DispatchUniqueness(
    string? ActiveKey,
    string? WindowKey,
    TimeSpan? Window)
{
    public override string ToString() => $"DispatchUniqueness(active: {ActiveKey is not null}, window: {Window})";
}

public sealed record BatchDispatchItem(IQueueJob Job, DispatchOptions? Options = null)
{
    public override string ToString() => $"BatchDispatchItem({Job.GetType().Name})";
}

public sealed record BatchDispatchOptions
{
    public bool ContinueOnConflict { get; init; }
}

public sealed record BatchItemFailure(
    int Ordinal,
    string Code,
    string Message,
    JobId? ConflictingJobId);

public sealed record BatchItemResult(
    int Ordinal,
    DispatchReceipt? Receipt,
    BatchItemFailure? Failure);

public sealed record BatchDispatchReceipt(
    DispatchOperationId OperationId,
    IReadOnlyList<BatchItemResult> Items,
    string ConfirmedAcknowledgementProfile);

/// <summary>Durably dispatches jobs. Cancellation stops waiting; it never cancels an accepted job.</summary>
public interface IJobDispatcher
{
    Task<DispatchReceipt> DispatchAsync<TJob>(
        TJob job,
        DispatchOptions? options = null,
        CancellationToken cancellationToken = default)
        where TJob : class, IQueueJob;

    Task<BatchDispatchReceipt> DispatchManyAsync(
        IReadOnlyList<BatchDispatchItem> items,
        BatchDispatchOptions? options = null,
        CancellationToken cancellationToken = default);
}

public static class AcknowledgementProfiles
{
    public const string PostgresSynchronousCommit = "postgres.synchronous-commit";

    /// <summary>A reply follows a commit with WAL and <c>synchronous=FULL</c> on the committing connection (20 §1).</summary>
    public const string SqliteWalFull = "sqlite.wal-synchronous-full";

    /// <summary>A reply follows the script's writes being appended and fsynced under <c>appendfsync always</c> on the checked primary (22 §1).</summary>
    public const string RedisAofAlways = "redis.aof-always-single-primary-v1";
}

/// <summary>What staging reported inside a caller's transaction. Makes no durability claim.</summary>
public sealed record StagedDispatch(
    JobId JobId,
    DispatchOperationId OperationId,
    DispatchDisposition Disposition,
    DuplicateMatch DuplicateMatch,
    bool PolicyDrift);

public sealed record StagedBatchItemResult(
    int Ordinal,
    StagedDispatch? Staged,
    BatchItemFailure? Failure);

public sealed record StagedBatch(
    DispatchOperationId OperationId,
    IReadOnlyList<StagedBatchItemResult> Items);
