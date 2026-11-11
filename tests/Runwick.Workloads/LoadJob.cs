using System.Globalization;
using System.Text.Json.Serialization;
using Runwick.Serialization;

namespace Runwick.Workloads;

/// <summary>Workload job: <see cref="Pad"/> sizes the payload (≈ 1 KiB for P-01, 07 §14.3).</summary>
public sealed record LoadJob(string Id, string Pad) : IQueueJob
{
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string[]? ExtraPad { get; init; }

    public static LoadJob Create(long sequence, int payloadBytes)
    {
        var id = sequence.ToString(CultureInfo.InvariantCulture);
        var padLength = Math.Max(0, payloadBytes - 32);
        if (padLength <= PayloadBounds.MaxStringBytes)
        {
            return new LoadJob(id, new string('x', padLength));
        }

        // Leave room for the property names, ID and array delimiters at the payload cap.
        var chunks = new List<string>();
        for (var remaining = payloadBytes - 128 - PayloadBounds.MaxStringBytes; remaining > 0; remaining -= PayloadBounds.MaxStringBytes)
        {
            chunks.Add(new string('x', Math.Min(remaining, PayloadBounds.MaxStringBytes)));
        }

        return new LoadJob(id, new string('x', PayloadBounds.MaxStringBytes)) { ExtraPad = [.. chunks] };
    }
}

/// <summary>Handler duration for this worker process (0 = no-op).</summary>
public sealed record LoadBehaviour(TimeSpan Delay);

internal sealed class LoadHandler(LoadBehaviour behaviour) : IJobHandler<LoadJob>
{
    public Task HandleAsync(LoadJob job, CancellationToken cancellationToken) =>
        behaviour.Delay <= TimeSpan.Zero ? Task.CompletedTask : Task.Delay(behaviour.Delay, cancellationToken);
}

internal static class LoadRegistration
{
    public const string Queue = "load";
    public const string JobName = "wl.load";

    public static void Register(RunwickBuilder r, int? typeCeiling)
    {
        var job = r.AddJob<LoadJob>().Named(JobName).ToQueue(Queue).WithMaxExecutions(3);
        if (typeCeiling is { } ceiling)
        {
            job.MaxConcurrent(ceiling);
        }
    }
}
