namespace Runwick.ReferenceModel;

/// <summary>The model's own notion of a job. Deliberately independent from src/.</summary>
public sealed record JobModel(
    Guid JobId,
    string State,
    int Generation,
    int StartedCount,
    long LifetimeExecutions,
    int ConsecutiveUnstarted,
    int MaxExecutions,
    int MaxUnstarted,
    Guid? AttemptId,
    Guid? Token,
    DateTimeOffset? LeaseExpiresAt,
    bool CancelRequested,
    long Reservations)
{
    public static JobModel Dispatched(Guid jobId, int maxExecutions = 3, int maxUnstarted = 5) =>
        new(jobId, States.Pending, 1, 0, 0, 0, maxExecutions, maxUnstarted, null, null, null, false, 0);

    public bool Owned => AttemptId is not null;
}

public static class States
{
    public const string Pending = "pending";
    public const string Reserved = "reserved";
    public const string Running = "running";
    public const string Retrying = "retrying";
    public const string Succeeded = "succeeded";
    public const string Failed = "failed";
    public const string Cancelled = "cancelled";

    public static readonly IReadOnlySet<string> All = new HashSet<string>
        { Pending, Reserved, Running, Retrying, Succeeded, Failed, Cancelled };

    public static readonly IReadOnlySet<string> Terminal = new HashSet<string> { Succeeded, Failed, Cancelled };

    public static readonly IReadOnlySet<string> Waiting = new HashSet<string> { Pending, Retrying };

    public static readonly IReadOnlySet<string> OwnedStates = new HashSet<string> { Reserved, Running };
}
