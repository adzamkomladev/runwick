namespace Runwick.ReferenceModel;

/// <summary>Store-neutral view of committed state, read in one REPEATABLE READ snapshot.</summary>
public sealed record StoreSnapshot(
    DateTimeOffset ReadAt,
    IReadOnlyList<JobRow> Jobs,
    IReadOnlyList<AttemptRow> Attempts,
    IReadOnlyList<PermitRow> Permits,
    IReadOnlyList<DispatchKeyRow> DispatchKeys,
    IReadOnlyList<ReceiptRow> Receipts)
{
    public JobRow Job(Guid jobId) => Jobs.Single(j => j.JobId == jobId);

    public IEnumerable<AttemptRow> AttemptsOf(Guid jobId) =>
        Attempts.Where(a => a.JobId == jobId).OrderBy(a => a.ReservationNumber);
}

public sealed record JobRow(
    Guid JobId,
    string State,
    int Generation,
    long Revision,
    int StartedCount,
    long LifetimeExecutions,
    int ConsecutiveUnstarted,
    int MaxExecutions,
    int MaxUnstarted,
    Guid? CurrentAttemptId,
    Guid? OwnershipToken,
    Guid? WorkerInstanceId,
    DateTimeOffset? LeaseExpiresAt,
    bool CancelRequested,
    DateTimeOffset EligibleAt,
    string Queue,
    string? TerminalReason);

public sealed record AttemptRow(
    Guid AttemptId,
    Guid JobId,
    int Generation,
    long ReservationNumber,
    int? ExecutionNumber,
    string? Outcome,
    DateTimeOffset ReservedAt,
    DateTimeOffset? BegunAt,
    DateTimeOffset? ClosedAt);

public sealed record PermitRow(Guid ScopeId, Guid JobId, Guid AttemptId, DateTimeOffset ExpiresAt);

public sealed record DispatchKeyRow(string Kind, string Key, Guid JobId, bool Active, DateTimeOffset? WindowEndsAt);

public sealed record ReceiptRow(Guid CommandId, string Operation, string RetentionClass, bool HasResult);
