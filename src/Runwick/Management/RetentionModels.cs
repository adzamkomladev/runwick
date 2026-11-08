namespace Runwick.Management;

// SP-021 (17 §2, §7): the retention policy and the retention page.

/// <summary>
/// One policy per application (17 §2). <c>null</c> keeps forever; every value is whole hours, 1 h – 87,600 h; audit at
/// least 7 days; job history at least as long as both detail horizons. Fixed record classes (receipts, bindings, grants,
/// mappings) are not in the policy, so no policy can shorten them.
/// </summary>
public sealed record RetentionPolicy(
    TimeSpan? CompletedDetail,
    TimeSpan? FailedDetail,
    TimeSpan? JobHistory,
    TimeSpan? Audit,
    TimeSpan? ScalingEvents,
    TimeSpan? ScheduleOccurrences,
    TimeSpan WorkerPresence)
{
    public static RetentionPolicy Default { get; } = new(TimeSpan.FromDays(7), TimeSpan.FromDays(30), TimeSpan.FromDays(90), TimeSpan.FromDays(90),
        TimeSpan.FromDays(7), TimeSpan.FromDays(30), TimeSpan.FromHours(24));

    internal const int MaxHours = 87_600;

    /// <summary>The seven values in hours, in declaration order (null = forever).</summary>
    internal int?[] Hours() =>
        [H(CompletedDetail), H(FailedDetail), H(JobHistory), H(Audit), H(ScalingEvents), H(ScheduleOccurrences), H(WorkerPresence)];

    internal static RetentionPolicy FromHours(IReadOnlyList<int?> h) => new(T(h[0]), T(h[1]), T(h[2]), T(h[3]), T(h[4]), T(h[5]), T(h[6])!.Value);

    static int? H(TimeSpan? t) => t is { } v ? (int)Math.Clamp(v.TotalHours, int.MinValue, int.MaxValue) : null;

    static TimeSpan? T(int? hours) => hours is { } h ? TimeSpan.FromHours(h) : null;

    /// <summary>The first rule the policy breaks, or null (17 §2 rules).</summary>
    internal string? Problem()
    {
        TimeSpan?[] values = [CompletedDetail, FailedDetail, JobHistory, Audit, ScalingEvents, ScheduleOccurrences, WorkerPresence];
        foreach (var v in values)
        {
            if (v is { } t && (t.Ticks % TimeSpan.TicksPerHour != 0 || t < TimeSpan.FromHours(1) || t > TimeSpan.FromHours(MaxHours)))
            {
                return "Every retention value is whole hours between 1 and 87,600.";
            }
        }

        if (Audit < TimeSpan.FromDays(7))
        {
            return "Audit is kept at least 7 days.";
        }

        // Detail never outlives its job: null is infinity.
        return JobHistory is { } history && (CompletedDetail is not { } c || FailedDetail is not { } f || history < c || history < f)
            ? "Job history must be at least as long as both detail horizons."
            : null;
    }
}

/// <summary>Progress of one record class (17 §4.2). <see cref="Behind"/>: no pass caught up in the last 24 h.</summary>
public sealed record RetentionClassProgress(
    string RecordClass, string Kind, DateTimeOffset? LastBatchAt, DateTimeOffset? CaughtUpAt, long TotalRows,
    long TodayRows, int TodayBatches, int TodayFailures, string? LastErrorCode, DateTimeOffset? LastErrorAt, bool Behind);

/// <summary>The retention page: the policy in force (revision 0 = defaults, no edit yet) and per-class progress. No eligible-row counts.</summary>
public sealed record RetentionView(RetentionPolicy Policy, long Revision, IReadOnlyList<RetentionClassProgress> Classes, DateTimeOffset ObservedAt);

/// <summary>An edit's outcome. A rejection is a result: <c>stale_revision</c> (with <see cref="Revision"/> = actual) or <c>no_change</c>.</summary>
public sealed record RetentionPolicyResult(bool Succeeded, string? Code, long? Revision, RetentionPolicy? Policy, bool Replayed);
