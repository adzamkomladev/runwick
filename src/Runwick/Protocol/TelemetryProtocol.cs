namespace Runwick.Protocol;

/// <summary>One queue of an application-wide sample (18 §4): due waiting jobs counted up to 10,000, oldest due instant.</summary>
internal sealed record QueueSample(string Queue, long Due, DateTimeOffset? OldestDueAt);

/// <summary>An application-wide sample at store time <see cref="ObservedAt"/>.</summary>
internal sealed record ApplicationSample(DateTimeOffset ObservedAt, IReadOnlyList<QueueSample> Queues, long NeedsReview, DateTimeOffset? OldestOverdueAt);

/// <summary>
/// The collector's bounded, indexed reads (18 §4). Optional: a store without it reports no global metrics. No locks,
/// no receipt, no schema change.
/// </summary>
internal interface ITelemetrySampleStore
{
    public const int MaxDue = 10_000;

    Task<ApplicationSample> SampleAsync(string application, IReadOnlyList<string> queues, CancellationToken ct);
}
